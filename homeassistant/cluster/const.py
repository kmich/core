"""Constants for the cluster package."""

import re
from typing import Final

from homeassistant.const import (
    EVENT_CALL_SERVICE,
    EVENT_COMPONENT_LOADED,
    EVENT_CORE_CONFIG_UPDATE,
    EVENT_HOMEASSISTANT_CLOSE,
    EVENT_HOMEASSISTANT_FINAL_WRITE,
    EVENT_HOMEASSISTANT_START,
    EVENT_HOMEASSISTANT_STARTED,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_LOGGING_CHANGED,
    EVENT_SERVICE_REGISTERED,
    EVENT_SERVICE_REMOVED,
    EVENT_STATE_CHANGED,
    EVENT_STATE_REPORTED,
    EVENT_THEMES_UPDATED,
)

DOMAIN: Final = "cluster"

CONF_NODE_ID: Final = "node_id"
CONF_NATS_URL: Final = "nats_url"
CONF_NATS_USER: Final = "nats_user"
CONF_NATS_PASSWORD: Final = "nats_password"
CONF_NATS_TOKEN: Final = "nats_token"
CONF_NATS_CREDS: Final = "nats_creds"
CONF_NATS_CA: Final = "nats_ca"
CONF_ENCRYPTION_KEY: Final = "encryption_key"
CONF_LEASE_TTL: Final = "lease_ttl"
CONF_RENEW_INTERVAL: Final = "renew_interval"
CONF_UNLOAD_GRACE: Final = "unload_grace"
CONF_RPC_TIMEOUT: Final = "rpc_timeout"
CONF_MANAGED_ADDONS: Final = "managed_addons"
CONF_SKIP_ADDONS: Final = "skip_addons"
CONF_EXCLUDE_DOMAINS: Final = "exclude_domains"
CONF_STORAGE_EXCLUDE: Final = "storage_exclude"
CONF_SECURITY: Final = "security"
CONF_ADDON_SNAPSHOT_INTERVAL: Final = "addon_snapshot_interval"
CONF_ADDON_TAKEOVER_WAIT: Final = "addon_takeover_wait"
CONF_ADDON_SEED_NODE: Final = "addon_seed_node"
CONF_ADDON_BACKUP_LOCATION: Final = "addon_backup_location"
CONF_LOCAL_ADDONS: Final = "local_addons"
# In ``managed_addons``: every installed add-on but the local ones.
ADDON_ALL: Final = "all"
# Add-ons that run on every host (tools for the host itself), never managed
# with ``all``. ``local_addons`` adds to these.
DEFAULT_LOCAL_ADDONS: Final = frozenset(
    {
        "core_configurator",
        "core_ssh",
        "core_samba",
        "a0d7b954_ssh",
        "a0d7b954_vscode",
    }
)
# Keys of one item of ``managed_addons``.
CONF_SLUG: Final = "slug"
CONF_PATHS: Final = "paths"
CONF_EXCLUDE: Final = "exclude"
CONF_REPLICATE: Final = "replicate"
CONF_SEED_NODE: Final = "seed_node"

# How often the leader snapshots a replicated add-on (seconds).
DEFAULT_ADDON_SNAPSHOT_INTERVAL: Final = 900.0
# How long a new leader waits for a node that ran a managed add-on and stopped
# reporting (dead host, or dead Home Assistant on a live host) before it starts
# the add-on itself.
DEFAULT_ADDON_TAKEOVER_WAIT: Final = 90.0
# Supervisor add-on slugs (``core_mosquitto``, ``45df7312_zigbee2mqtt``) are
# used in KV keys.
ADDON_SLUG_RE: Final = re.compile(r"^[a-z0-9_]{1,64}$")
# Snapshots of replicated add-ons in ``hass_storage``: ``addon.<slug>`` is the
# manifest, ``addon.<slug>.s<slot>.<n>`` the chunks.
ADDON_KEY_PREFIX: Final = "addon."

# ``strict``: TLS (or a loopback address) and credentials are required.
SECURITY_STRICT: Final = "strict"
SECURITY_PERMISSIVE: Final = "permissive"
DEFAULT_SECURITY: Final = SECURITY_STRICT
CONF_REPLICAS: Final = "replicas"

DEFAULT_NATS_URL: Final = "nats://localhost:4222"
DEFAULT_LEASE_TTL: Final = 3.0
DEFAULT_RENEW_INTERVAL: Final = 1.0
DEFAULT_UNLOAD_GRACE: Final = 2.0
DEFAULT_RPC_TIMEOUT: Final = 60.0
DEFAULT_REPLICAS: Final = 1

# Seconds to wait for the first NATS connection during setup before
# continuing in the background (entries stay on standby until connected).
INITIAL_CONNECT_TIMEOUT: Final = 5.0

# Node ids are used verbatim as a NATS subject token and inside KV values.
NODE_ID_RE: Final = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

BUCKET_STATES: Final = "hass_states"
BUCKET_LEASES: Final = "hass_leases"
BUCKET_STORAGE: Final = "hass_storage"

# The version of what nodes write for each other: member keys, leases and
# config entry records (``protocol``). A node works with its own version only;
# a key or record without the field is version 1. See ``protocol.py``.
PROTOCOL_VERSION: Final = 1

# The oldest NATS server the cluster runs on (the line its tests run on).
MIN_NATS_VERSION: Final = (2, 15)

# The leadership term in ``hass_storage``. Each new leader increases it before
# it reads or writes anything else there, and every write to ``hass_storage``
# is conditional on the revision of that write (the fencing token): once
# another node took over, the server refuses the old leader's writes.
TERM_KEY: Final = "cluster.term"

# Lease key held by the cluster leader: the node that runs automations and
# scripts and owns every entity without a config entry (YAML entities).
LEADER_KEY: Final = "_leader"

# Every node renews ``_node.<node_id>`` in the lease bucket: cluster
# membership, including nodes that hold no lease (a pure standby).
MEMBER_KEY_PREFIX: Final = "_node."

# A held entry whose setup keeps failing on this node (SETUP_RETRY or
# SETUP_ERROR) for this long is handed to another node, if there is one.
# This node then does not take it back for HANDOFF_COOLDOWN seconds.
FAILED_SETUP_HANDOFF_AFTER: Final = 10.0
HANDOFF_COOLDOWN: Final = 60.0

# Name of the system users that run calls forwarded from another node for a
# user this node does not know, one per role ("... (admin)" in the admin
# group, "... (user)" in the user group, "... (read_only)" in the read-only
# group). Core checks what the call starts as it would for the caller's group.
REMOTE_USER_NAME: Final = "Cluster remote user"

# The sender is a token of every subject a node publishes to, so that NATS
# permissions can restrict each node's user to its own (see the README): the
# receiver takes the sender from the subject, never from the message body.
RPC_SUBJECT: Final = "hass.rpc.{to}.from.{origin}"
RPC_SUBJECT_TO: Final = "hass.rpc.{node_id}.from.*"
EVENT_SUBJECT: Final = "hass.event.{node_id}"
EVENT_SUBJECT_ALL: Final = "hass.event.*"
# Replies (RPC answers, JetStream acknowledgements) come to this inbox.
INBOX_PREFIX: Final = "_INBOX.{node_id}"

# RPC calls a node runs at the same time; more are answered "busy" at once.
MAX_INBOUND_RPC: Final = 32

STANDBY_WAITING_FOR_COORDINATOR: Final = (
    "Cluster standby: waiting for the cluster coordinator"
)

# Services of these domains always execute on the node that was called.
LOCAL_SERVICE_DOMAINS: Final = frozenset(
    {
        "cluster",
        "frontend",
        "hassio",
        "homeassistant",
        "logger",
        "persistent_notification",
        "recorder",
        "system_log",
    }
)

# Services of the ``homeassistant`` domain that act on entities or config
# entries: routed to the node that runs them, not executed locally.
ROUTED_HOMEASSISTANT_SERVICES: Final = frozenset(
    {"reload_config_entry", "toggle", "turn_off", "turn_on", "update_entity"}
)

# Domains whose entities run on the leader only (automations, scripts).
# Calls of these services execute on the leader, except the ones in
# LEADER_DOMAIN_BROADCAST_SERVICES, which change configuration or the
# enabled state that every node keeps for when it becomes leader.
LEADER_DOMAINS: Final = frozenset({"automation", "script"})
LEADER_DOMAIN_BROADCAST_SERVICES: Final = {
    "automation": frozenset({"reload", "turn_on", "turn_off", "toggle"}),
    "script": frozenset({"reload"}),
}

# Events that describe this node, not the house: never replicated.
# State changes are replicated through the state bucket instead.
LOCAL_EVENT_TYPES: Final = frozenset(
    {
        EVENT_CALL_SERVICE,
        EVENT_COMPONENT_LOADED,
        EVENT_CORE_CONFIG_UPDATE,
        EVENT_HOMEASSISTANT_CLOSE,
        EVENT_HOMEASSISTANT_FINAL_WRITE,
        EVENT_HOMEASSISTANT_START,
        EVENT_HOMEASSISTANT_STARTED,
        EVENT_HOMEASSISTANT_STOP,
        EVENT_LOGGING_CHANGED,
        EVENT_SERVICE_REGISTERED,
        EVENT_SERVICE_REMOVED,
        EVENT_STATE_CHANGED,
        EVENT_STATE_REPORTED,
        EVENT_THEMES_UPDATED,
        "area_registry_updated",
        "automation_reloaded",
        "category_registry_updated",
        "config_entry_discovered",
        "data_entry_flow_progressed",
        "device_registry_updated",
        "entity_registry_updated",
        "floor_registry_updated",
        "label_registry_updated",
        "labs_updated",
        "lovelace_updated",
        "panels_updated",
        "persistent_notifications_updated",
        "recorder_5min_statistics_generated",
        "recorder_hourly_statistics_generated",
        "repairs_issue_registry_updated",
        "scene_reloaded",
        "system_log_event",
        "timer_out_of_sync",
        "user_added",
        "user_removed",
        "user_updated",
    }
)

# The only stores the leader mirrors (as ``store.<key>``): what a standby
# reads at takeover. Nothing else leaves the node through the storage mirror;
# config entries are published as records of their own, and the UI files and
# helpers by the UI config sync. Private stores (credentials, tokens, keys)
# are never passed to the store write listener at all.
MIRRORED_STORES: Final = frozenset(
    {
        "core.restore_state",
        "core.floor_registry",
        "core.area_registry",
        "core.label_registry",
        "core.category_registry",
        "core.device_registry",
        "core.entity_registry",
    }
)

STORAGE_KEY_PREFIX: Final = "store."
# The unload grace each node advertised (outlives the expiring lease keys).
GRACE_KEY_PREFIX: Final = "grace."
ENTRY_KEY_PREFIX: Final = "entry."
CONFIG_ENTRIES_STORE: Final = "core.config_entries"
