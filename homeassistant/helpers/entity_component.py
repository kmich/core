"""Helpers for components that manage entities."""

import asyncio
from collections.abc import Callable, Coroutine, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
import logging
from types import ModuleType
from typing import TYPE_CHECKING, Any

from homeassistant import config as conf_util
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    CONF_ENTITY_NAMESPACE,
    CONF_SCAN_INTERVAL,
    EVENT_HOMEASSISTANT_STOP,
)
from homeassistant.core import (
    CALLBACK_TYPE,
    EntityServiceResponse,
    Event,
    HassJobType,
    HomeAssistant,
    ServiceCall,
    SupportsResponse,
    callback,
)
from homeassistant.exceptions import (
    ConfigValidationError,
    HomeAssistantError,
    ServiceValidationError,
)
from homeassistant.loader import async_get_integration
from homeassistant.setup import async_prepare_setup_platform
from homeassistant.util.hass_dict import HassKey

from . import cluster, discovery, entity, service
from .entity_platform import EntityPlatform
from .typing import ConfigType, DiscoveryInfoType, VolDictType, VolSchemaType

DEFAULT_SCAN_INTERVAL = timedelta(seconds=15)

type _PlatformRequest = tuple[str, ConfigType, DiscoveryInfoType | None]
type _PlatformKey = tuple[str, timedelta | None, str | None]
DATA_INSTANCES: HassKey[dict[str, EntityComponent]] = HassKey("entity_components")


def _idle_event() -> asyncio.Event:
    event = asyncio.Event()
    event.set()
    return event


@dataclass(slots=True)
class _LeaderOnlyPlatforms:
    """Platforms of a component that run on the cluster leader only."""

    # Not set up because this instance is not the leader.
    deferred: list[_PlatformRequest] = field(default_factory=list)
    # Set up while this instance is the leader, by platform key.
    running: dict[_PlatformKey, list[_PlatformRequest]] = field(default_factory=dict)
    # Changes of leadership are applied one at a time.
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # Setups that passed the leadership check, and an event set while none.
    setups: int = 0
    idle: asyncio.Event = field(default_factory=_idle_event)


async def async_update_entity(hass: HomeAssistant, entity_id: str) -> None:
    """Trigger an update for an entity."""
    domain = entity_id.partition(".")[0]
    entity_comp = hass.data.get(DATA_INSTANCES, {}).get(domain)

    if entity_comp is None:
        logging.getLogger(__name__).warning(
            "Forced update failed. Component for %s not loaded.", entity_id
        )
        return

    if (entity_obj := entity_comp.get_entity(entity_id)) is None:
        logging.getLogger(__name__).warning(
            "Forced update failed. Entity %s not found.", entity_id
        )
        return

    await entity_obj.async_update_ha_state(True)


class EntityComponent[_EntityT: entity.Entity = entity.Entity]:
    """The EntityComponent manages platforms that manage entities.

    An example of an entity component is 'light', which manages platforms such
    as 'hue.light'.

    This class has the following responsibilities:
     - Process the configuration and set up a platform based component,
       for example light.
     - Manage the platforms and their entities.
     - Help extract the entities from a service call.
     - Listen for discovery events for platforms related to the domain.
    """

    def __init__(
        self,
        logger: logging.Logger,
        domain: str,
        hass: HomeAssistant,
        scan_interval: timedelta = DEFAULT_SCAN_INTERVAL,
    ) -> None:
        """Initialize an entity component."""
        self.logger = logger
        self.hass = hass
        self.domain = domain
        self.scan_interval = scan_interval

        self.config: ConfigType | None = None

        self._platforms: dict[
            str | tuple[str, timedelta | None, str | None], EntityPlatform
        ] = {}
        # In a cluster, platforms set up from YAML or by discovery run on the
        # leader only (None without a cluster).
        self._leader_only: _LeaderOnlyPlatforms | None = None
        self._unsub_leadership: CALLBACK_TYPE | None = None
        domain_platform = self._async_init_entity_platform(domain, domain, None)
        self.async_add_entities = domain_platform.async_add_entities
        self.add_entities = domain_platform.add_entities
        self._entities: dict[str, _EntityT] = domain_platform.domain_entities  # type: ignore[assignment]
        hass.data.setdefault(DATA_INSTANCES, {})[domain] = self  # type: ignore[assignment]

    @property
    def entities(self) -> Iterable[_EntityT]:
        """Return an iterable that returns all entities.

        As the underlying dicts may change when async context is lost,
        callers that iterate over this asynchronously should make a copy
        using list() before iterating.
        """
        return self._entities.values()

    def get_entity(self, entity_id: str) -> _EntityT | None:
        """Get an entity."""
        return self._entities.get(entity_id)

    def register_shutdown(self) -> None:
        """Register shutdown on Home Assistant STOP event.

        Note: this is only required if the integration never calls
        `setup` or `async_setup`.
        """
        self.hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, self._async_shutdown)

    def setup(self, config: ConfigType) -> None:
        """Set up a full entity component.

        This doesn't block the executor to protect from deadlocks.
        """
        self.hass.create_task(
            self.async_setup(config), f"EntityComponent setup {self.domain}"
        )

    async def async_setup(self, config: ConfigType) -> None:
        """Set up a full entity component.

        Loads the platforms from the config and will listen for supported
        discovered platforms.

        This method must be run in the event loop.
        """
        self.register_shutdown()

        self.config = config

        if self._unsub_leadership is None and cluster.async_is_clustered(self.hass):
            if self._leader_only is None:
                self._leader_only = _LeaderOnlyPlatforms()
            self._unsub_leadership = cluster.async_track_leadership(
                self.hass, self._async_leadership_changed
            )

        # Look in config for Domain, Domain 2, Domain 3 etc and load them
        for p_type, p_config in conf_util.config_per_platform(config, self.domain):
            if p_type is not None:
                self.hass.async_create_task_internal(
                    self.async_setup_platform(p_type, p_config),
                    f"EntityComponent setup platform {p_type} {self.domain}",
                    eager_start=True,
                )

        # Generic discovery listener for loading platform dynamically
        # Refer to: homeassistant.helpers.discovery.async_load_platform()
        discovery.async_listen_platform(
            self.hass, self.domain, self._async_component_platform_discovered
        )

    async def _async_component_platform_discovered(
        self, platform: str, info: dict[str, Any] | None
    ) -> None:
        """Handle the loading of a platform."""
        await self.async_setup_platform(platform, {}, info)

    async def async_setup_entry(self, config_entry: ConfigEntry) -> bool:
        """Set up a config entry."""
        platform_type = config_entry.domain
        platform = await async_prepare_setup_platform(
            self.hass,
            # In future PR we should make hass_config part of the constructor
            # params.
            self.config or {},
            self.domain,
            platform_type,
        )

        if platform is None:
            return False

        key = config_entry.entry_id

        if key in self._platforms:
            raise ValueError(
                f"Config entry {config_entry.title} ({key}) for "
                f"{platform_type}.{self.domain} has already been setup!"
            )

        entity_platform = self._async_init_entity_platform(
            key,
            platform_type,
            platform,
            scan_interval=getattr(platform, "SCAN_INTERVAL", None),
        )

        return await entity_platform.async_setup_entry(config_entry)

    async def async_unload_entry(self, config_entry: ConfigEntry) -> bool:
        """Unload a config entry."""
        key = config_entry.entry_id

        if key not in self._platforms:
            self.logger.warning(
                (
                    "Ignored unload request for config entry %s (%s) in %s.%s; "
                    "no platform is loaded, it was never set up "
                    "or has already been unloaded"
                ),
                config_entry.title,
                key,
                config_entry.domain,
                self.domain,
            )
            return True

        await self._async_destroy_platform(key)
        return True

    async def _async_destroy_platform(
        self, key: str | tuple[str, timedelta | None, str | None]
    ) -> None:
        """Remove an owned platform from the component and global registry."""
        platform = self._platforms.pop(key)
        await platform.async_destroy()

    async def async_extract_from_service(
        self, service_call: ServiceCall, expand_group: bool = True
    ) -> list[_EntityT]:
        """Extract all known and available entities from a service call.

        Will return an empty list if entities specified but unknown.

        This method must be run in the event loop.
        """
        return await service.async_extract_entities(
            self.entities, service_call, expand_group
        )

    @callback
    def async_register_entity_service(
        self,
        name: str,
        schema: VolDictType | VolSchemaType | None,
        func: str | Callable[..., Any],
        required_features: list[int] | None = None,
        supports_response: SupportsResponse = SupportsResponse.NONE,
        *,
        admin_only: bool = False,
        description_placeholders: Mapping[str, str] | None = None,
    ) -> None:
        """Register an entity service."""
        service.async_register_entity_service(
            self.hass,
            self.domain,
            name,
            admin_only=admin_only,
            entities=self._entities,
            func=func,
            job_type=HassJobType.Coroutinefunction,
            required_features=required_features,
            schema=schema,
            supports_response=supports_response,
            description_placeholders=description_placeholders,
        )

    @callback
    def async_register_batched_entity_service(
        self,
        name: str,
        schema: VolDictType | VolSchemaType | None,
        func: Callable[
            [list[_EntityT], ServiceCall],
            Coroutine[Any, Any, EntityServiceResponse | None],
        ],
        required_features: Iterable[int] | None = None,
        supports_response: SupportsResponse = SupportsResponse.NONE,
        *,
        description_placeholders: Mapping[str, str] | None = None,
    ) -> None:
        """Register a batched entity service.

        A batched entity service calls the service function once with all
        matching entities as a list, instead of once per entity.
        """
        service.async_register_batched_entity_service(
            self.hass,
            self.domain,
            name,
            entities=self._entities,
            func=func,
            required_features=required_features,
            schema=schema,
            supports_response=supports_response,
            description_placeholders=description_placeholders,
        )

    async def async_setup_platform(
        self,
        platform_type: str,
        platform_config: ConfigType,
        discovery_info: DiscoveryInfoType | None = None,
    ) -> None:
        """Set up a platform for this component."""
        if self.config is None:
            raise RuntimeError("async_setup needs to be called first")

        # In a cluster, a platform set up from YAML or by discovery runs on the
        # leader only.
        if (
            leader_only := self._leader_only
        ) is None or cluster.async_domain_runs_on_every_instance(
            self.hass, platform_type
        ):
            await self._async_setup_platform(
                platform_type, platform_config, discovery_info
            )
            return

        request = (platform_type, platform_config, discovery_info)
        if not cluster.async_is_leader(self.hass):
            leader_only.deferred.append(request)
            return
        # Counted before the first await: a demotion waits for this setup to
        # finish, then takes the platform down again.
        leader_only.setups += 1
        leader_only.idle.clear()
        try:
            await self._async_setup_platform(
                platform_type, platform_config, discovery_info, leader_only
            )
        finally:
            leader_only.setups -= 1
            if not leader_only.setups:
                leader_only.idle.set()

    async def _async_setup_platform(
        self,
        platform_type: str,
        platform_config: ConfigType,
        discovery_info: DiscoveryInfoType | None,
        leader_only: _LeaderOnlyPlatforms | None = None,
    ) -> None:
        """Set up a platform; ``leader_only`` for one that runs on the leader."""
        if TYPE_CHECKING:
            assert self.config is not None
        platform = await async_prepare_setup_platform(
            self.hass, self.config, self.domain, platform_type
        )

        if platform is None:
            return

        # Use config scan interval, fallback to platform if none set
        scan_interval = platform_config.get(
            CONF_SCAN_INTERVAL, getattr(platform, "SCAN_INTERVAL", None)
        )
        entity_namespace = platform_config.get(CONF_ENTITY_NAMESPACE)

        key = (platform_type, scan_interval, entity_namespace)

        if leader_only is not None:
            request = (platform_type, platform_config, discovery_info)
            if not cluster.async_is_leader(self.hass):
                # Leadership was lost while the platform was prepared.
                leader_only.deferred.append(request)
                return
            leader_only.running.setdefault(key, []).append(request)

        if key not in self._platforms:
            self._async_init_entity_platform(
                key, platform_type, platform, scan_interval, entity_namespace
            )

        await self._platforms[key].async_setup(platform_config, discovery_info)

    @callback
    def _async_leadership_changed(self, leader: bool) -> None:
        """Set up or take down the platforms that run on the leader only."""
        if (leader_only := self._leader_only) is None or not (
            leader_only.deferred or leader_only.running or leader_only.setups
        ):
            return
        task = self.hass.async_create_task_internal(
            self._async_apply_leadership(leader_only),
            f"EntityComponent leadership {self.domain}",
            eager_start=True,
        )
        if not leader:
            # The former leader runs these platforms until the task is done.
            cluster.async_add_transition_task(
                self.hass, task, f"taking down {self.domain} platforms"
            )

    async def _async_apply_leadership(self, leader_only: _LeaderOnlyPlatforms) -> None:
        """Make the platforms match the current leadership.

        The tasks of one component run one at a time, and each looks at the
        leadership as it is when it runs, so a quick change back and forth
        ends in the right state.
        """
        async with leader_only.lock:
            if cluster.async_is_leader(self.hass):
                deferred, leader_only.deferred = leader_only.deferred, []
                for request in deferred:
                    await self.async_setup_platform(*request)
                return
            # Setups that passed the leadership check finish first; each is
            # in ``running`` (or deferred again) once it is done.
            await leader_only.idle.wait()
            left_running: list[str] = []
            for key in list(leader_only.running):
                requests = leader_only.running.pop(key)
                leader_only.deferred.extend(requests)
                if (platform := self._platforms.get(key)) is None:
                    continue
                await self._async_destroy_platform(key)
                left_running.extend(platform.failed_removals)
            if left_running:
                # Entity removal logs its errors and goes on; these entities
                # did not stop.
                raise HomeAssistantError(
                    f"Entities of {self.domain} platforms did not stop:"
                    f" {', '.join(left_running)}"
                )

    async def _async_reset(self) -> None:
        """Remove entities and reset the entity component to initial values.

        This method must be run in the event loop.
        """
        tasks = []

        for key, platform in self._platforms.items():
            if key == self.domain:
                tasks.append(platform.async_reset())
            else:
                tasks.append(self._async_destroy_platform(key))

        if tasks:
            await asyncio.gather(*tasks)

        # A reload starts from the new configuration.
        if self._leader_only is not None:
            self._leader_only.deferred.clear()
            self._leader_only.running.clear()
        self.config = None

    async def async_remove_entity(self, entity_id: str) -> None:
        """Remove an entity managed by one of the platforms."""
        found = None

        for platform in self._platforms.values():
            if entity_id in platform.entities:
                found = platform
                break

        if found:
            await found.async_remove_entity(entity_id)

    async def async_prepare_reload(self, *, skip_reset: bool = False) -> ConfigType:
        """Prepare reloading this entity component.

        This method is intended to be called from service handlers implementing reload.
        Will raise ServiceValidationError if the config is not valid.
        """
        try:
            conf = await conf_util.async_hass_config_yaml(self.hass)
        except HomeAssistantError as err:
            raise ServiceValidationError(
                f"Failed to load configuration: {err}"
            ) from err

        integration = await async_get_integration(self.hass, self.domain)

        try:
            processed_conf = await conf_util.async_process_component_and_handle_errors(
                self.hass, conf, integration, raise_on_failure=True
            )
        except ConfigValidationError as err:
            raise ServiceValidationError(
                translation_domain=err.translation_domain,
                translation_key=err.translation_key,
                translation_placeholders=err.translation_placeholders,
            ) from err

        if not skip_reset:
            await self._async_reset()

        return processed_conf

    @callback
    def _async_init_entity_platform(
        self,
        key: str | tuple[str, timedelta | None, str | None],
        platform_type: str,
        platform: ModuleType | None,
        scan_interval: timedelta | None = None,
        entity_namespace: str | None = None,
    ) -> EntityPlatform:
        """Create and register a platform owned by this component."""
        if key in self._platforms:
            raise ValueError(f"Platform {key} is already registered")
        if scan_interval is None:
            scan_interval = self.scan_interval

        entity_platform = EntityPlatform(
            hass=self.hass,
            logger=self.logger,
            domain=self.domain,
            platform_name=platform_type,
            platform=platform,
            scan_interval=scan_interval,
            entity_namespace=entity_namespace,
        )
        entity_platform.async_prepare()
        self._platforms[key] = entity_platform
        return entity_platform

    @callback
    def _async_shutdown(self, event: Event) -> None:
        """Call when Home Assistant is stopping."""
        if self._unsub_leadership is not None:
            self._unsub_leadership()
            self._unsub_leadership = None
        for platform in self._platforms.values():
            platform.async_shutdown()
