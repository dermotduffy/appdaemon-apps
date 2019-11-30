import datetime
import functools
import os
import traceback

import appdaemon.plugins.hass.hassapi as hass
import voluptuous as vol

import conditions

CONF_TRIGGER_ON_CONDITION = 'trigger_on_condition'
CONF_TRIGGER_OFF_CONDITION = 'trigger_off_condition'
CONF_EXTEND_CONDITION = 'extend_condition'
CONF_CONDITION = 'condition'
CONF_ACTIVATE_ENTITIES = 'activate_entities'
CONF_DEACTIVATE_ENTITIES = 'deactivate_entities'
CONF_STATE_ENTITIES = 'state_entities'
CONF_AUTO_TIMEOUT = 'auto_timeout'
CONF_HARD_TIMEOUT = 'hard_timeout'
CONF_OUTPUT = 'output'
CONF_ENTITY_ID = 'entity_id'
CONF_SERVICE = 'service'
CONF_ON_STATE = 'on_state'
CONF_STATUS_VAR = 'status_var'

DEFAULT_AUTO_TIMEOUT = 60*15
DEFAULT_HARD_TIMEOUT = 60*60*3
DEFAULT_ON_STATE = 'on'

KEY_FRIENDLY_NAME = 'friendly_name'
KEY_ON = 'on'
KEY_OFF = 'off'

STATUS_VAR_UPDATE_SECONDS = 10
STATUS_VAR_UNKNOWN = 'unknown'
STATUS_VAR_MANUAL = 'manual'
STATUS_VAR_ACTIVE_TIMER = 'auto_timer'
STATUS_VAR_WAITING = 'waiting'
STATUS_VAR_ATTR_TIME_REMAINING = 'light_timeout'
STATUS_VAR_ATTR_NONE = 'N/A'
STATUS_VAR_ATTR_LAST_TRIGGER = 'last_trigger_%s'
STATUS_VAR_ATTR_EXTEND = 'will_extend'
STATUS_VAR_ATTR_EXTEND_NEVER = 'never'
STATUS_VAR_ATTR_EXTEND_NO = 'no'
STATUS_VAR_ATTR_EXTEND_YES = 'yes'

CONFIG_CONDITION_SCHEMA = vol.Schema([conditions.CONFIG_CONDITION_BASE_SCHEMA], extra=vol.PREVENT_EXTRA)
ALLOWED_SERVICES = ['turn_on', 'turn_off']

ENTITY_SCHEMA = vol.Schema({
  vol.Required(CONF_ENTITY_ID): str,
  vol.Optional(CONF_ON_STATE, default=DEFAULT_ON_STATE): str,
}, extra=vol.PREVENT_EXTRA)
ACTIVATE_ENTITIES = ENTITY_SCHEMA.extend({
  vol.Optional(CONF_SERVICE, default='turn_on'): vol.In(ALLOWED_SERVICES),
}, extra=vol.PREVENT_EXTRA)
GEACTIVATE_ENTITIES = ENTITY_SCHEMA.extend({
  vol.Optional(CONF_SERVICE, default='turn_off'): vol.In(ALLOWED_SERVICES),
}, extra=vol.PREVENT_EXTRA)

OUTPUT_SCHEMA = vol.Schema([{
  vol.Optional(CONF_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Required(CONF_ACTIVATE_ENTITIES): [ACTIVATE_ENTITIES],
  vol.Optional(CONF_DEACTIVATE_ENTITIES, default=[]): [DEACTIVATE_ENTITIES],
}])

CONFIG_SCHEMA = vol.Schema({
  vol.Optional(CONF_STATUS_VAR): str,
  vol.Optional(CONF_TRIGGER_ON_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_TRIGGER_OFF_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_EXTEND_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_STATE_ENTITIES): [ENTITY_SCHEMA],
  vol.Optional(CONF_AUTO_TIMEOUT, default=DEFAULT_AUTO_TIMEOUT): vol.Range(min=60),
  vol.Optional(CONF_HARD_TIMEOUT, default=DEFAULT_HARD_TIMEOUT): vol.Range(min=300),
  vol.Required(CONF_OUTPUT): OUTPUT_SCHEMA,
}, extra=vol.ALLOW_EXTRA)

@functools.total_ordering
class Timer(object):
  def __init__(self, app, func, seconds, name='timer', kwargs=None):
    self._app = app
    self._func = func
    self._seconds = seconds
    self._name = name
    self._kwargs = kwargs

    self._handle = None
    self._expire_datetime = None

  def create(self):
    if self._handle is not None:
      self.cancel()
    self._expire_datetime = self._app.datetime() + datetime.timedelta(
        seconds=self._seconds)
    self._handle = self._app.run_in(
        lambda kwargs: self._log_wrap(self._func, self._kwargs),
        self._seconds)
    self._app.log('Created timer: (%s, %s) for %i seconds' % (
        self._name, self._handle, self._seconds))

  def cancel(self):
    if self._handle:
      self._app.log('Cancel timer: (%s, %s)' % (self._name, self._handle))
      self._app.cancel_timer(self._handle)
      self._raw_reset()

  def _raw_reset(self):
    self._handle = None
    self._expire_datetime = None

  def get_time_until_expire_string(self):
    total_seconds = 0
    if self._expire_datetime is not None:
      total_seconds = (
          self._expire_datetime - self._app.datetime()).total_seconds()
    hours, remainder = divmod(total_seconds, 60*60)
    minutes, seconds = divmod(remainder, 60)
    return '{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))

  def _log_wrap(self, func, kwargs):
    try:
      result = func(kwargs)
      self._raw_reset()
      return result
    except Exception as e:
      # Funnel exceptions through the Appdaemon logger (otherwise we won't see
      # them at all)
      stack_trace = traceback.format_exc()
      self.log('%s%s%s' % (e, os.linesep, stack_trace), level="ERROR")

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


class AutoLights(hass.Hass):
  def initialize(self):
    self._manual_mode = False
    self._last_trigger = {
        KEY_ON: None,
        KEY_OFF: None
    }

    self._config = CONFIG_SCHEMA(self.args)
    self._status_var = self._config.get(CONF_STATUS_VAR)

    self._auto_timer = Timer(self, self._auto_timer_expire,
        self._config.get(CONF_AUTO_TIMEOUT), name='auto')
    self._hard_timer = Timer(self, self._hard_timer_expire,
        self._config.get(CONF_HARD_TIMEOUT), name='hard')

    trigger_on_entities = conditions.extract_entities_from_condition(
        self._config.get(CONF_TRIGGER_ON_CONDITION))
    self.log('Trigger on entities -> %s' % trigger_on_entities)

    for entity_id in trigger_on_entities:
      self.listen_state(self._trigger_callback, entity_id, on=True)

    trigger_off_entities = conditions.extract_entities_from_condition(
        self._config.get(CONF_TRIGGER_OFF_CONDITION))
    self.log('Trigger off entities -> %s' % trigger_off_entities)

    for entity_id in trigger_off_entities:
      self.listen_state(self._trigger_callback, entity_id, on=False)

    self._state_entities = self._get_state_entities()
    self.log('State entities -> %s' % self._state_entities)

    for entity in self._state_entities:
      self.listen_state(self._state_callback, entity[CONF_ENTITY_ID])

    if self._has_on_state_entity():
      self._hard_timer.create()

    if self._status_var:
      self.run_every(
          self._update_status,
          self.datetime(),
          STATUS_VAR_UPDATE_SECONDS)

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
      state = STATUS_VAR_WAITING
      attributes = {
          STATUS_VAR_ATTR_TIME_REMAINING: STATUS_VAR_ATTR_NONE,
          STATUS_VAR_ATTR_LAST_TRIGGER % KEY_ON: STATUS_VAR_ATTR_NONE,
          STATUS_VAR_ATTR_LAST_TRIGGER % KEY_OFF: STATUS_VAR_ATTR_NONE,
          STATUS_VAR_ATTR_EXTEND: STATUS_VAR_ATTR_EXTEND_NEVER,
      }

      if self._manual_mode:
        state = STATUS_VAR_MANUAL
      elif self._auto_timer:
        state = STATUS_VAR_ACTIVE_TIMER

      timers = sorted((self._auto_timer, self._hard_timer))
      if timers[0]:
        attributes[STATUS_VAR_ATTR_TIME_REMAINING] = (
            timers[0].get_time_until_expire_string())

      for key in (KEY_ON, KEY_OFF):
        if self._last_trigger[key]:
          attributes[STATUS_VAR_ATTR_LAST_TRIGGER % key] = (
              self._last_trigger[key])

      if self._config.get(CONF_EXTEND_CONDITION):
        if self._should_extend():
          attributes[STATUS_VAR_ATTR_EXTEND] = STATUS_VAR_ATTR_EXTEND_YES
        else:
          attributes[STATUS_VAR_ATTR_EXTEND] = STATUS_VAR_ATTR_EXTEND_NO

      self.set_state(self._status_var, state=state, attributes=attributes)

  def _should_extend(self):
    return (self._config.get(CONF_EXTEND_CONDITION) and
        conditions.evaluate_condition(
            self, self.datetime().time(),
            self._config.get(CONF_EXTEND_CONDITION)))

  def _auto_timer_expire(self, kwargs):
    self.log('Auto timer expired at %s' % self.datetime())

    if self._should_extend():
      self.log('Extending auto timer ...')
      self._auto_timer.create()
      return

    output = self._get_best_matching_output()
    if output:
      self._turn_off(output)

  def _hard_timer_expire(self, kwargs):
    self.log('Hard timer expired at %s' % self.datetime())

    output = self._get_best_matching_output()
    if output:
      self._turn_off(output)

  def _turn_on(self, output):
    self.log('Turning on output: %s' % output)
    for entity in output[CONF_ACTIVATE_ENTITIES]:
      self.turn_on(entity[CONF_ENTITY_ID])

  def _turn_off(self, output):
    self.log('Turning off output: %s' % output)
    if output.get(CONF_DEACTIVATE_ENTITIES):
      entities = output[CONF_DEACTIVATE_ENTITIES]
    else:
      entities = output[CONF_ACTIVATE_ENTITIES]

    for entity in entities:
      self.turn_off(entity[CONF_ENTITY_ID])

  def _has_on_state_entity(self):
    for entity in self._state_entities:
      if self.get_state(entity[CONF_ENTITY_ID]) == entity[CONF_ON_STATE]:
        return True
    return False

  def _state_callback(self, entity, attribute, old, new, kwargs):
    self.log('State callback: %s (old: %s, new: %s)' % (entity, old, new))
    if self._has_on_state_entity():
      # A changing state entity resets the timers.
      self._hard_timer.create()

      if not self._auto_timer:
        # Effectively moves into manual mode.
        self._manual_mode = True
        self._last_trigger = { KEY_ON: None, KEY_OFF: None }
      else:
        self._auto_timer.create()
    else:
      self._auto_timer.cancel()
      self._hard_timer.cancel()
      self._manual_mode = False

      # Don't reset the last OFF trigger as it may have turned the lights off.
      self._last_trigger[KEY_ON] = None

    self._update_status()

  def _trigger_callback(self, entity, attribute, old, new, kwargs):
    on = kwargs[KEY_ON]
    if on:
      on_or_off = KEY_ON
    else:
      on_or_off = KEY_OFF

    self.log('Trigger %s callback: %s (old: %s, new: %s)' % (
        on_or_off, entity, old, new))

    if self._manual_mode:
      self.log('Skipping trigger on (%s) due to manual mode...' % entity)
      return

    if on:
      condition = self._config.get(CONF_TRIGGER_ON_CONDITION)
    else:
      condition = self._config.get(CONF_TRIGGER_OFF_CONDITION)

    triggered = conditions.evaluate_condition(self, self.datetime().time(),
        condition, triggers={entity: new})

    if triggered:
      output = self._get_best_matching_output()
      if output:
        if on:
          self._auto_timer.create()
          self._hard_timer.create()
          self._turn_on(output)
        else:
          self._turn_off(output)
        self._last_trigger[on_or_off] = self.get_state(
            entity, attribute=KEY_FRIENDLY_NAME)

    self._update_status()
