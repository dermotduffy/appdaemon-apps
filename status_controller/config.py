import copy
import datetime

import voluptuous as vol

import conditions

KEY_STATE = 'state'
KEY_ATTRIBUTES = 'attributes'
KEY_ALL_ATTRIBUTES = 'all'
KEY_BRIGHTNESS = 'brightness'
KEY_TRANSITION = 'transition'

# From https://www.home-assistant.io/components/light/
# All allowable arguments for light turn on.
ARGS_FOR_TURN_ON = [
    KEY_TRANSITION,
    'profile',
    'hs_color',
    'xy_color',
    'rgb_color',
    'white_value',
    'color_temp',
    'kelvin',
    'color_name',
    KEY_BRIGHTNESS,
    'brightness_pct',
    'flash',
    'effect',
]

# Allowable attributes retrieved from state that can be used for turn on.
ATTR_ARGS_FOR_TURN_ON = [
    KEY_TRANSITION,
    'profile',
    'rgb_color',
    'white_value',
    'color_temp',
    KEY_BRIGHTNESS,
    'flash',
    'effect',
]

# Allow arguments for light turn_off.
ARGS_FOR_TURN_OFF = [
    KEY_TRANSITION,
]

CONF_SETTINGS = 'settings'
CONF_PRIORITY = 'priority'
CONF_TAGS = 'tags'
CONF_OUTPUTS = 'outputs'
CONF_LIGHT = 'light'
CONF_SONOS = 'sonos'
CONF_NOTIFY = 'notify'
CONF_MQTT = 'mqtt'
CONF_COLOR_NAME = 'color_name'
CONF_SONOS_VOLUME = 'volume'
CONF_ENTITIES = 'entities'
CONF_ENTITY_ID = 'entity_id'
CONF_UNDERLYING_ENTITIES = 'underlying_entities'
CONF_UNDERLYING_ENTITY_IDS = 'underlying_entity_ids'
CONF_EVENT_NAME = 'event_name'
CONF_EVENT = 'event'
CONF_ARGUMENTS = 'arguments'
CONF_BREATH_LENGTH = 'breath_length'      # Length of a single breath.
CONF_LENGTH = 'length'

CONF_ACTION = 'action'
CONF_FINISH_ACTION = 'finish_action'

# Interrupt isn't really an action, it has no action class. It's just a placeholder
# that serves to interrupt an existing action (and potentially kill it, e.g.
# stop media from playing by force ending an action).
CONF_ACTION_INTERRUPT = 'interrupt'
CONF_ACTION_LIGHT_RESTORE = 'restore'

CONF_ACTION_NONE = 'none'
CONF_ACTION_LIGHT_BREATHE = 'breathe'
CONF_ACTION_LIGHT_TURN_ON = 'turn_on'
CONF_ACTION_LIGHT_TURN_OFF = 'turn_off'
CONF_ACTION_LIGHT_TOGGLE = 'toggle'
CONF_ACTION_LIGHT_SPEECH = 'speech'

CONF_ACTION_SONOS_TTS = 'speak'
CONF_ACTION_SONOS_MEDIA_PLAY = 'media_play'

CONF_ACTION_MQTT_TOPIC = 'topic'
CONF_ACTION_MQTT_PAYLOAD = 'payload'

CONF_FORCE = 'force'
CONF_MESSAGE = 'message'
CONF_TITLE = 'title'
CONF_SERVICE = 'service'
CONF_CONDITION = 'condition'
CONF_TAG = 'tag'

CONF_SONOS_TTS_SERVICE = 'tts_service'
CONF_SONOS_MEDIA = 'media'
CONF_SONOS_CHIME = 'chime'
CONF_SONOS_CHIME_LENGTH = 'chime_length'

LIGHT_ACTIONS = [
    CONF_ACTION_LIGHT_TURN_ON,
    CONF_ACTION_LIGHT_TURN_OFF,
    CONF_ACTION_LIGHT_TOGGLE,
    CONF_ACTION_LIGHT_BREATHE,
    CONF_ACTION_LIGHT_SPEECH,
]

LIGHT_FINISH_ACTIONS = [
    CONF_ACTION_NONE,
    CONF_ACTION_LIGHT_TURN_ON,
    CONF_ACTION_LIGHT_TURN_OFF,
    CONF_ACTION_LIGHT_RESTORE,
    CONF_ACTION_INTERRUPT,
]

SONOS_ACTIONS = [
    CONF_ACTION_SONOS_TTS,
    CONF_ACTION_SONOS_MEDIA_PLAY,
    CONF_ACTION_INTERRUPT,
]

# The set of keys that may vary whilst still forming a sonos group -- sonos
# media players will be joined if these are the only keys that differ. Other
# differing keys will result in the creation of different groups.
SONOS_GROUP_IGNORE_KEYS = [
    CONF_ENTITIES,
    CONF_SONOS_VOLUME,
    CONF_PRIORITY,
]

DEFAULT_LIGHT_ACTION = CONF_ACTION_LIGHT_TOGGLE
DEFAULT_LIGHT_FINISH_ACTION = CONF_ACTION_NONE
DEFAULT_LIGHT_LENGTH = 5
DEFAULT_LIGHT_BREATH_LENGTH = 2

LIGHT_SPEECH_HIGHEST_BRIGHTNESS = 200
LIGHT_SPEECH_LOWEST_BRIGHTNESS = 80

DEFAULT_SONOS_ACTION = CONF_ACTION_SONOS_TTS
DEFAULT_SONOS_LENGTH = 5
DEFAULT_SONOS_TTS_SERVICE = 'tts/google_cloud_say'
DEFAULT_MESSAGE = 'The message was unset'
DEFAULT_SONOS_CHIME_LENGTH = 3

DEFAULT_MQTT_SERVICE = 'mqtt/publish'
DEFAULT_MQTT_PAYLOAD = '{{ tags|tojson }}'

DEFAULT_FORCE = False
MIN_PRIORITY = 0
MAX_PRIORITY = 100
DEFAULT_PRIORITY = MIN_PRIORITY

# We cannot use default= in the schema, as otherwise arguments overrides
# would incorrectly override user-specified values with defaults in
# get_event_mapping.
DEFAULTS_MAPPING = {
  CONF_LIGHT: {
    CONF_ACTION: DEFAULT_LIGHT_ACTION,
    CONF_FINISH_ACTION: DEFAULT_LIGHT_FINISH_ACTION,
    CONF_LENGTH: DEFAULT_LIGHT_LENGTH,
    CONF_PRIORITY: DEFAULT_PRIORITY,
  },
  CONF_SONOS: {
    CONF_ACTION: DEFAULT_SONOS_ACTION,
    CONF_LENGTH: DEFAULT_SONOS_LENGTH,
    CONF_SONOS_TTS_SERVICE: DEFAULT_SONOS_TTS_SERVICE,
    CONF_MESSAGE: DEFAULT_MESSAGE,
    CONF_SONOS_CHIME_LENGTH: DEFAULT_SONOS_CHIME_LENGTH,
    CONF_PRIORITY: DEFAULT_PRIORITY,
  },
  CONF_NOTIFY: {
    CONF_PRIORITY: DEFAULT_PRIORITY,
    CONF_MESSAGE: DEFAULT_MESSAGE,
  },
  CONF_MQTT: {
    CONF_PRIORITY: DEFAULT_PRIORITY,
    CONF_SERVICE: DEFAULT_MQTT_SERVICE,
    CONF_ACTION_MQTT_PAYLOAD: DEFAULT_MQTT_PAYLOAD,
  },
  CONF_SETTINGS: {
    CONF_FORCE: DEFAULT_FORCE,
  },
}

CONFIG_SCHEMA_SETTINGS_ATTR = vol.Schema({
  vol.Optional(CONF_FORCE): bool,
})

CONFIG_SCHEMA_SHARED_ATTR = vol.Schema({
  vol.Optional(CONF_PRIORITY, default=DEFAULT_PRIORITY):
      vol.Range(min=MIN_PRIORITY, max=MAX_PRIORITY),
})

# Cannot use defaults, as otherwise parameter overriding will not function
# correctly (as non-present attributes will appear present and override
# actually present attributes).
CONFIG_SCHEMA_LIGHT_ATTR = CONFIG_SCHEMA_SHARED_ATTR.extend({
  vol.Optional(CONF_LENGTH): vol.Range(min=0),
  vol.Optional(CONF_ACTION): vol.In(LIGHT_ACTIONS),
  vol.Optional(CONF_FINISH_ACTION): vol.In(LIGHT_FINISH_ACTIONS),
  vol.Optional(CONF_BREATH_LENGTH): vol.Range(min=2, max=10.0),
}, extra=vol.ALLOW_EXTRA)

CONFIG_SCHEMA_SONOS_ATTR = CONFIG_SCHEMA_SHARED_ATTR.extend({
  vol.Optional(CONF_LENGTH): vol.Range(min=0),
  vol.Optional(CONF_SONOS_VOLUME): vol.Range(min=0.0, max=1.0),
  vol.Optional(CONF_MESSAGE): str,
  vol.Optional(CONF_SONOS_TTS_SERVICE): str,
  vol.Optional(CONF_SONOS_MEDIA): str,
  vol.Optional(CONF_ACTION): vol.In(SONOS_ACTIONS),
  vol.Optional(CONF_SONOS_CHIME): str,
  vol.Optional(CONF_SONOS_CHIME_LENGTH): vol.Range(min=0),
}, extra=vol.PREVENT_EXTRA)

CONFIG_SCHEMA_NOTIFY_ATTR = CONFIG_SCHEMA_SHARED_ATTR.extend({
  vol.Optional(CONF_MESSAGE): str,
  vol.Optional(CONF_TITLE): str,
}, extra=vol.PREVENT_EXTRA)

CONFIG_SCHEMA_MQTT_ATTR = CONFIG_SCHEMA_SHARED_ATTR.extend({
  vol.Required(CONF_ACTION_MQTT_TOPIC): str,
  vol.Optional(CONF_ACTION_MQTT_PAYLOAD): str,
}, extra=vol.PREVENT_EXTRA)

def ConstrainTime(fmt='%H:%M:%S'):
  return lambda v: datetime.datetime.strptime(v, fmt).time()

def ConstrainTimeRange(fmt='%H:%M:%S'):
  return lambda v: tuple(
      datetime.datetime.strptime(t, fmt).time() for t in v.split('-'))

CONFIG_CONDITION_BASE_SCHEMA = copy.copy(conditions.CONFIG_CONDITION_BASE_SCHEMA)
CONFIG_CONDITION_BASE_SCHEMA.update({
  vol.Optional(CONF_TAG): str,
})
CONFIG_CONDITION_SCHEMA = vol.Schema([
  CONFIG_CONDITION_BASE_SCHEMA
])

CONFIG_UNDERLYING_ENTITIES_SCHEMA = vol.Schema({
  vol.Optional(CONF_LIGHT): vol.Schema({
    str: [str],
  }, extra=vol.PREVENT_EXTRA),
}, extra=vol.PREVENT_EXTRA)

CONFIG_SCHEMA_OUTPUT = vol.Schema({
  vol.Optional(CONF_SETTINGS): CONFIG_SCHEMA_SETTINGS_ATTR,
  vol.Optional(CONF_LIGHT): vol.Schema([
    CONFIG_SCHEMA_LIGHT_ATTR.extend({
      vol.Required(CONF_ENTITIES): [str],
    })
  ]),
  vol.Optional(CONF_SONOS): vol.Schema([
    CONFIG_SCHEMA_SONOS_ATTR.extend({
      vol.Required(CONF_ENTITIES): [str],
    })
  ]),
  vol.Optional(CONF_NOTIFY): vol.Schema([
    CONFIG_SCHEMA_NOTIFY_ATTR.extend({
      vol.Required(CONF_SERVICE): str,
    })
  ]),
  vol.Optional(CONF_MQTT): vol.Schema([
    CONFIG_SCHEMA_MQTT_ATTR.extend({
      vol.Optional(CONF_SERVICE): str,
    })
  ]),
  vol.Optional(CONF_CONDITION): CONFIG_CONDITION_SCHEMA,
}, extra=vol.PREVENT_EXTRA)

CONFIG_SCHEMA = vol.Schema({
  # The name of the HASS event to listen to.
  vol.Required(CONF_EVENT_NAME): str,
  vol.Optional(CONF_TAGS): vol.Schema({
    vol.Optional(str): vol.Schema(vol.Any(None, {
      vol.Optional(CONF_SETTINGS): CONFIG_SCHEMA_SETTINGS_ATTR,
      vol.Optional(CONF_LIGHT): CONFIG_SCHEMA_LIGHT_ATTR,
      vol.Optional(CONF_SONOS): CONFIG_SCHEMA_SONOS_ATTR,
      vol.Optional(CONF_NOTIFY): CONFIG_SCHEMA_NOTIFY_ATTR,
      vol.Optional(CONF_MQTT): CONFIG_SCHEMA_MQTT_ATTR,
    })),
  }, extra=vol.PREVENT_EXTRA),
  vol.Optional(CONF_UNDERLYING_ENTITIES): CONFIG_UNDERLYING_ENTITIES_SCHEMA,
  vol.Optional(CONF_OUTPUTS): vol.Schema([
    CONFIG_SCHEMA_OUTPUT
  ]),
}, extra=vol.ALLOW_EXTRA)

EVENT_SCHEMA = vol.Schema({
  vol.Required(CONF_TAGS): vol.Schema([str]),
  vol.Optional(CONF_SETTINGS): CONFIG_SCHEMA_SETTINGS_ATTR,
  vol.Optional(CONF_LIGHT): CONFIG_SCHEMA_LIGHT_ATTR,
  vol.Optional(CONF_SONOS): CONFIG_SCHEMA_SONOS_ATTR,
  vol.Optional(CONF_NOTIFY): CONFIG_SCHEMA_NOTIFY_ATTR,
  vol.Optional(CONF_MQTT): CONFIG_SCHEMA_MQTT_ATTR,
}, extra=vol.PREVENT_EXTRA)

def get_event_arguments(config, event, output_args, domain):
  args = {}

  # Args in the event are the default.
  if domain in event:
    args = copy.copy(event.get(domain))

  # Overwrite event args with tag args.
  event_tags = event.get(CONF_TAGS)
  tags = config.get(CONF_TAGS)
  for event_tag in event_tags:
    if tags and event_tag in tags:
      if tags[event_tag] and domain in tags[event_tag]:
        tag_args = tags[event_tag][domain]
        for arg in tag_args:
          args[arg] = tag_args[arg]

  # Finally, overwrite with output entity args,
  # which take absolute priority.
  if output_args:
    for arg in output_args:
      args[arg] = output_args[arg]

  # Populate some defaults if they are missing.
  for key in DEFAULTS_MAPPING[domain]:
    if key not in args:
      args[key] = DEFAULTS_MAPPING[domain][key]

  return args

def log(app, obj, message):
  if type(obj) == type:
    name = obj.__name__
  else:
    name = type(obj).__name__
  app.log('[%s]: %s' % (name, message))
