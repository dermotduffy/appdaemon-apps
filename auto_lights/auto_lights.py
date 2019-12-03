import datetime
import functools
import os
import traceback

import appdaemon.plugins.hass.hassapi as hass
import voluptuous as vol

import conditions

CONF_TRIGGER_ACTIVATE_CONDITION = 'trigger_activate_condition'
CONF_TRIGGER_DEACTIVATE_CONDITION = 'trigger_deactivate_condition'
CONF_EXTEND_CONDITION = 'extend_condition'
CONF_DISABLE_CONDITION = 'disable_condition'
CONF_CONDITION = 'condition'
CONF_ACTIVATE_ENTITIES = 'activate_entities'
CONF_DEACTIVATE_ENTITIES = 'deactivate_entities'
CONF_STATE_ENTITIES = 'state_entities'
CONF_AUTO_TIMEOUT = 'auto_timeout'
CONF_HARD_TIMEOUT = 'hard_timeout'
CONF_GRACE_PERIOD_TIMEOUT = 'grace_timeout'
CONF_OUTPUT = 'output'
CONF_ENTITY_ID = 'entity_id'
CONF_SERVICE = 'service'
CONF_ON_STATE = 'on_state'
CONF_STATUS_VAR = 'status_var'
CONF_MAX_ACTIONS_PER_MIN = 'max_actions_per_min'
CONF_SERVICE_DATA = 'service_data'

DEFAULT_AUTO_TIMEOUT = 60*15
DEFAULT_HARD_TIMEOUT = 60*60*3
DEFAULT_GRACE_PERIOD_TIMEOUT = 30
DEFAULT_ON_STATE = 'on'
DEFAULT_STATE_UPDATE_TIMEOUT=5
DEFAULT_MAX_ACTIONS_PER_MIN=4

KEY_FRIENDLY_NAME = 'friendly_name'
KEY_ACTIVATE = 'activate'
KEY_DEACTIVATE = 'deactivate'

STATUS_VAR_UPDATE_SECONDS = 10
STATUS_VAR_STATE_MANUAL = 'manual'
STATUS_VAR_STATE_ACTIVE_TIMER = 'auto_timer'
STATUS_VAR_STATE_WAITING = 'waiting'
STATUS_VAR_STATE_PAUSED = 'paused'
STATUS_VAR_STATE_DISABLED = 'disabled'
STATUS_VAR_ATTR_NA = 'N/A'
STATUS_VAR_ATTR_NONE = 'None'
STATUS_VAR_ATTR_TIME_REMAINING = 'light_timeout'
STATUS_VAR_ATTR_LAST_TRIGGER = 'last_trigger_%s'
STATUS_VAR_ATTR_EXTEND = 'will_extend'
STATUS_VAR_ATTR_EXTEND_NEVER = 'never'
STATUS_VAR_ATTR_NO = 'no'
STATUS_VAR_ATTR_YES = 'yes'
STATUS_VAR_ATTR_DISABLED = 'disabled'
STATUS_VAR_ATTR_ICON = 'icon'

STATUS_VAR_ICONS = {
    STATUS_VAR_STATE_MANUAL: 'mdi:hand-left',
    STATUS_VAR_STATE_ACTIVE_TIMER: 'mdi:timer',
    STATUS_VAR_STATE_WAITING: 'mdi:sleep',
    STATUS_VAR_STATE_PAUSED: 'mdi:pause',
    STATUS_VAR_STATE_DISABLED: 'mdi:block-helper',
}

CONFIG_CONDITION_SCHEMA = vol.Schema(
    [conditions.CONFIG_CONDITION_BASE_SCHEMA],
    extra=vol.PREVENT_EXTRA)

SERVICE_TURN_ON = 'turn_on'
SERVICE_TURN_OFF = 'turn_off'
VALID_SERVICES = (SERVICE_TURN_ON, SERVICE_TURN_OFF)

SERVICE_DATA = vol.Schema({
}, extra=vol.ALLOW_EXTRA)

ENTITY_SCHEMA = vol.Schema({
  vol.Required(CONF_ENTITY_ID): str,
  vol.Optional(CONF_ON_STATE, default=DEFAULT_ON_STATE): str,
}, extra=vol.PREVENT_EXTRA)
ACTIVATE_ENTITIES = ENTITY_SCHEMA.extend({
  vol.Optional(CONF_SERVICE, default=SERVICE_TURN_ON): vol.In(VALID_SERVICES),
  vol.Optional(CONF_SERVICE_DATA, default={}): SERVICE_DATA,
}, extra=vol.ALLOW_EXTRA)
DEACTIVATE_ENTITIES = ENTITY_SCHEMA.extend({
  vol.Optional(CONF_SERVICE, default=SERVICE_TURN_OFF): vol.In(VALID_SERVICES),
  vol.Optional(CONF_SERVICE_DATA, default={}): SERVICE_DATA,
}, extra=vol.ALLOW_EXTRA)

OUTPUT_SCHEMA = vol.Schema([{
  vol.Optional(CONF_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Required(CONF_ACTIVATE_ENTITIES): [ACTIVATE_ENTITIES],
  vol.Optional(CONF_DEACTIVATE_ENTITIES, default=[]): [DEACTIVATE_ENTITIES],
}])

CONFIG_SCHEMA = vol.Schema({
  vol.Optional(CONF_STATUS_VAR): str,
  vol.Optional(CONF_TRIGGER_ACTIVATE_CONDITION,
               default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_TRIGGER_DEACTIVATE_CONDITION,
               default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_EXTEND_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_DISABLE_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_STATE_ENTITIES): [ENTITY_SCHEMA],
  vol.Optional(CONF_AUTO_TIMEOUT,
               default=DEFAULT_AUTO_TIMEOUT): vol.Range(min=60),
  vol.Optional(CONF_HARD_TIMEOUT,
               default=DEFAULT_HARD_TIMEOUT): vol.Range(min=300),
  vol.Optional(CONF_GRACE_PERIOD_TIMEOUT,
               default=DEFAULT_GRACE_PERIOD_TIMEOUT): vol.Range(min=0),
  vol.Optional(CONF_MAX_ACTIONS_PER_MIN,
               default=DEFAULT_MAX_ACTIONS_PER_MIN): vol.Range(min=0),

  vol.Required(CONF_OUTPUT): OUTPUT_SCHEMA,
}, extra=vol.ALLOW_EXTRA)

def timedelta_to_str(td):
  hours, remainder = divmod(td.total_seconds(), 60*60)
  minutes, seconds = divmod(remainder, 60)
  return '{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))

@functools.total_ordering
class Timer(object):
  def __init__(self, app, func=None, seconds=None, name='timer', kwargs=None):
    self._app = app
    self._func = func
    self._seconds = seconds
    self._name = name
    self._kwargs = kwargs

    self._handle = None
    self._expire_datetime = None

  def create(self, seconds=None):
    if seconds is None:
      seconds = self._seconds
    if seconds is None:
      raise RuntimeError('Failed to specify timer \'seconds\'')

    if self._handle is not None:
      self.cancel()
    self._expire_datetime = self._app.datetime() + datetime.timedelta(
        seconds=seconds)
    self._handle = self._app.run_in(
        lambda kwargs: self._log_wrap(self._func, self._kwargs),
        seconds)
    self._app.log('Created timer: (%s, %s) for %i seconds' % (
        self._name, self._handle, seconds))

  def cancel(self):
    if self._handle:
      self._app.log('Cancel timer: (%s, %s)' % (self._name, self._handle))
      self._app.cancel_timer(self._handle)
      self._raw_reset()

  def _raw_reset(self):
    self._handle = None
    self._expire_datetime = None

  def get_time_until_expire_string(self):
    if self._expire_datetime is None:
      return timedelta_to_str(datetime.timedelta(0))
    return timedelta_to_str(self._expire_datetime - self._app.datetime())

  def _log_wrap(self, func, kwargs):
    try:
      # Reset internals first so callbacks can see that timer has finished.
      self._raw_reset()
      if func:
        func(kwargs)
    except Exception as e:
      # Funnel exceptions through the Appdaemon logger (otherwise we won't see
      # them at all)
      stack_trace = traceback.format_exc()
      self._app.log('%s%s%s' % (e, os.linesep, stack_trace), level="ERROR")

  def __eq__(self, other):
    return self._expire_datetime == other._expire_datetime
  def __lt__(self, other):
    if other._expire_datetime is None:
      return True
    return (self._expire_datetime is not None and
        self._expire_datetime < other._expire_datetime)
  def __bool__(self):
    return self._handle is not None
  def __repr__(self):
    return '<Timer:%s,%s>' % (self._name, self.get_time_until_expire_string())

# A note on state: As much as possible, attempt to store the authoritative
# state in HA (retrieved via Appdaemon get_state(), not here.

class AutoLights(hass.Hass):
  def initialize(self):
    self._manual_mode = False
    self._last_actions = []
    self._last_trigger = {
        KEY_ACTIVATE: None,
        KEY_DEACTIVATE: None
    }

    self._config = CONFIG_SCHEMA(self.args)
    self._status_var = self._config.get(CONF_STATUS_VAR)

    self._auto_timer = Timer(self, self._auto_timer_expire,
        self._config.get(CONF_AUTO_TIMEOUT), name='auto')
    self._hard_timer = Timer(self, self._hard_timer_expire,
        self._config.get(CONF_HARD_TIMEOUT), name='hard')
    self._pause_timer = Timer(self, self._pause_timer_expire, name='pause')
    self._state_update_timer = Timer(self,
        seconds=DEFAULT_STATE_UPDATE_TIMEOUT, name='state_update')

    self._listen_condition('activate', CONF_TRIGGER_ACTIVATE_CONDITION,
        self._trigger_callback, activate=True)
    self._listen_condition('deactivate', CONF_TRIGGER_DEACTIVATE_CONDITION,
        self._trigger_callback, activate=False)
    self._listen_condition('extend', CONF_EXTEND_CONDITION,
        self._extend_callback)
    self._listen_condition('disable', CONF_DISABLE_CONDITION,
        self._disable_callback)

    self._state_entities = self._get_state_entities()
    self._listen_entities('state',
        [entity[CONF_ENTITY_ID] for entity in self._state_entities],
        self._state_callback)

    if self._has_on_state_entity():
      self._hard_timer.create()

    if self._status_var:
      self.run_every(
          self._update_status,
          self.datetime(),
          STATUS_VAR_UPDATE_SECONDS)

  def _listen_condition(self, name, conf_condition, func, **kwargs):
    entities = conditions.extract_entities_from_condition(
        self._config.get(conf_condition))
    return self._listen_entities(name, entities, func, **kwargs)

  def _listen_entities(self, name, entities, func, **kwargs):
    self.log('Listening to %s entities -> %s' % (name, entities))

    for entity_id in entities:
      self.listen_state(func, entity_id, **kwargs)
    return entities

  def _get_state_entities(self):
    if CONF_STATE_ENTITIES in self._config:
      return self._config[CONF_STATE_ENTITIES]
    state_entities = []
    for output in self._config[CONF_OUTPUT]:
      for activate_entity in output[CONF_ACTIVATE_ENTITIES]:
        state_entities.append(activate_entity)
      for deactivate_entity in output[CONF_DEACTIVATE_ENTITIES]:
        state_entities.append(deactivate_entity)
    return state_entities

  def _get_best_matching_output(self):
    for output in self._config.get(CONF_OUTPUT):
      if conditions.evaluate_condition(
          self, self.datetime().time(), output.get(CONF_CONDITION)):
        return output
    return None

  def _update_status(self, kwargs=None):
    if self._status_var:
      state = STATUS_VAR_STATE_WAITING
      attributes = {
          STATUS_VAR_ATTR_TIME_REMAINING: STATUS_VAR_ATTR_NA,
          STATUS_VAR_ATTR_LAST_TRIGGER % KEY_ACTIVATE: STATUS_VAR_ATTR_NONE,
          STATUS_VAR_ATTR_LAST_TRIGGER % KEY_DEACTIVATE: STATUS_VAR_ATTR_NONE,
          STATUS_VAR_ATTR_EXTEND: STATUS_VAR_ATTR_EXTEND_NEVER,
          STATUS_VAR_ATTR_DISABLED: STATUS_VAR_ATTR_NO,
      }
      if self._is_disabled():
        state = STATUS_VAR_STATE_DISABLED
      elif self._pause_timer:
        state = STATUS_VAR_STATE_PAUSED
      elif self._manual_mode:
        state = STATUS_VAR_STATE_MANUAL
      elif self._auto_timer:
        state = STATUS_VAR_STATE_ACTIVE_TIMER
      attributes[STATUS_VAR_ATTR_ICON] = STATUS_VAR_ICONS[state]

      timers = sorted((self._auto_timer, self._hard_timer))
      if timers[0]:
        attributes[STATUS_VAR_ATTR_TIME_REMAINING] = (
            timers[0].get_time_until_expire_string())

      for key in (KEY_ACTIVATE, KEY_DEACTIVATE):
        if self._last_trigger[key]:
          attributes[STATUS_VAR_ATTR_LAST_TRIGGER % key] = (
              self._last_trigger[key])

      if self._config.get(CONF_EXTEND_CONDITION):
        if self._should_extend():
          attributes[STATUS_VAR_ATTR_EXTEND] = STATUS_VAR_ATTR_YES
        else:
          attributes[STATUS_VAR_ATTR_EXTEND] = STATUS_VAR_ATTR_NO

      if self._config.get(CONF_DISABLE_CONDITION) and self._is_disabled():
          attributes[STATUS_VAR_ATTR_DISABLED] = STATUS_VAR_ATTR_YES

      self.set_state(self._status_var, state=state, attributes=attributes)

  def _should_extend(self):
    return (self._config.get(CONF_EXTEND_CONDITION) and
        conditions.evaluate_condition(
            self, self.datetime().time(),
            self._config.get(CONF_EXTEND_CONDITION)))

  def _is_disabled(self):
    return (self._config.get(CONF_DISABLE_CONDITION) and
        conditions.evaluate_condition(
            self, self.datetime().time(),
            self._config.get(CONF_DISABLE_CONDITION)))

  def _auto_timer_expire(self, kwargs):
    self.log('Auto timer expired at %s' % self.datetime())

    if self._should_extend():
      self.log('Extending auto timer ...')
      self._auto_timer.create()
      return

    output = self._get_best_matching_output()
    if output:
      self._deactivate(output)

  def _hard_timer_expire(self, kwargs):
    self.log('Hard timer expired at %s' % self.datetime())

    output = self._get_best_matching_output()
    if output:
      self._deactivate(output)

  def _deactivate(self, output):
    return self._activate(output, activate=False)

  def _activate(self, output, activate=True):
    self.log('%s output: %s' % (
        'Activating' if activate else 'Deactivating', output))

    override_service = None
    override_data = None

    if activate:
      entities = output[CONF_ACTIVATE_ENTITIES]
    else:
      if output[CONF_DEACTIVATE_ENTITIES]:
        entities = output[CONF_DEACTIVATE_ENTITIES]
      else:
        # If deactivation entities are not provided, go with the activation
        # entities, however override the service to be turn_off, and remove the
        # data (as it will otherwise cause an off call to fail).
        entities = output[CONF_ACTIVATE_ENTITIES]
        override_service = SERVICE_TURN_OFF
        override_data = {}

    if entities:
      self._state_update_timer.create()

    for entity in entities:
      data = (override_data if override_data is not None else
          entity.get(CONF_SERVICE_DATA, {}))
      service = (override_service if override_service is not None else
          entity.get[CONF_SERVICE])
      if service == SERVICE_TURN_ON:
        self.turn_on(entity[CONF_ENTITY_ID], **data)
      else:
        self.turn_off(entity[CONF_ENTITY_ID], **data)

    self._last_actions.insert(0, (self.datetime(), activate))

  def _prune_last_actions(self):
    # Only keep 1 minute worth of last actions.
    for tpl in self._last_actions:
      if self._seconds_since_dt(tpl[0]) >= 60:
        self._last_actions.remove(tpl)

  def _distinct_last_actions(self):
    last_activate = None
    distinct = 0
    for (dt, activate) in self._last_actions:
      if last_activate is None:
        distinct = 1
      elif last_activate != activate:
        distinct += 1
      last_activate = activate
    return distinct

  def _has_on_state_entity(self):
    for entity in self._state_entities:
      if self.get_state(entity[CONF_ENTITY_ID]) == entity[CONF_ON_STATE]:
        return True
    return False

  def _state_callback(self, entity, attribute, old, new, kwargs):
    self.log('State callback: %s (old: %s, new: %s)' % (entity, old, new))

    if self._is_disabled():
      self.log('Disabled: Ignoring state for: %s' % entity)
      return

    # A note on manual mode: Manual mode is not enabled when any
    # state change happens during automated lighting. The assumption is that
    # automated lighting will be the norm for a room, and so automations that
    # impact that lighting do not constitute conversion to manual mode (e.g.
    # status controller events).  Automations that work outside of automated
    # lighting times will indeed convert this to manual mode.
    if self._has_on_state_entity():
      # A changing state entity resets the timers.
      self._hard_timer.create()

      if not self._auto_timer:
        # If there's a light on, but we're not automated lighting, then it's
        # manual mode (see note above).
        self._manual_mode = True
        self.log('Changed to manual mode: %s (%s->%s)' % (entity, old, new))
    else:
      self._auto_timer.cancel()
      self._hard_timer.cancel()
      self._manual_mode = False

    # If this state change was not due to an action invoked from this app, then
    # pause triggers for <grace_period>.
    if not self._state_update_timer:
      self._pause_timer.create(
          seconds=self._config.get(CONF_GRACE_PERIOD_TIMEOUT))
    self._update_status()

  def _seconds_since_dt(self, dt):
    return (self.datetime() - dt).total_seconds()

  def _within_window(self, dt, window):
    return self._seconds_since_dt(dt) < window

  def _trigger_callback(self, entity, attribute, old, new, kwargs):
    activate = kwargs[KEY_ACTIVATE]
    self.log('Trigger callback (activate=%s): %s (old: %s, new: %s)' % (
        activate, entity, old, new))

    if self._is_disabled():
      self.log('Disabled: Skipping trigger for: %s' % entity)
      return
    elif self._pause_timer:
      self.log('Paused: Skipping trigger for: %s' % entity)
      return
    elif self._manual_mode:
      self.log('Manual mode: Skipping trigger for: %s' % entity)
      return

    condition = self._config.get(
        CONF_TRIGGER_ACTIVATE_CONDITION if activate
        else CONF_TRIGGER_DEACTIVATE_CONDITION)
    triggered = conditions.evaluate_condition(self, self.datetime().time(),
        condition, triggers={entity: new})

    activate_key = KEY_ACTIVATE if activate else KEY_DEACTIVATE

    if triggered:
      output = self._get_best_matching_output()
      if output:
        # Safety precaution: Pause changes if more distinct actions than
        # max_actions_per_min (avoid lights flapping due to more configuration
        # choices).  (e.g. imagine a trigger than turns lights on when
        # brightness dips below X, but turns them off when it rises above X: a
        # poorly configured instance could cause the lights to flap)
        # Implicitly, this is allowing multiple repitions of the same action
        # with no pauseing (e.g. repeatedly turning on the same light due to
        # walking past multiple motion sensors is just fine).
        self.log('Last-actions: %s' % self._last_actions)

        # Prune actions to only the last 1 minute worth.
        self._prune_last_actions()
        max_actions_per_min = self._config.get(CONF_MAX_ACTIONS_PER_MIN)

        if self._distinct_last_actions() >= max_actions_per_min:
          self.log('Pausing attempts to %s output as >%i (%s) distinct '
                   'actions have been executed in the last minute: %s' % (
              activate_key,
              max_actions_per_min,
              CONF_MAX_ACTIONS_PER_MIN,
              output))
          # Pause for 1 minute (it's max actions per minute).
          self._pause_timer.create(seconds=1*60)
          self._update_status()
          return

        if activate:
          self._auto_timer.create()
        self._activate(output, activate=activate)

        self._last_trigger[activate_key] = self.get_state(
            entity, attribute=KEY_FRIENDLY_NAME)

        self._update_status()

  def _pause_timer_expire(self, kwargs):
    self._update_status()

  def _extend_callback(self, entity, attribute, old, new, kwargs):
    self._update_status()

  def _disable_callback(self, entity, attribute, old, new, kwargs):
    if self._is_disabled():
      self._auto_timer.cancel()
      self._hard_timer.cancel()
    else:
      if self._has_on_state_entity() and not self._hard_timer:
        self._hard_timer.create()
      self._manual_mode = False

    self._update_status()
