import appdaemon.plugins.hass.hassapi as hass
import datetime
import voluptuous as vol

import conditions

CONF_SUPPRESS_CONDITION = 'suppress_condition'
CONF_TRIGGER_CONDITION = 'trigger_condition'
CONF_DISABLE_CONDITION = 'disable_condition'
CONF_WINDOW_SECONDS = 'window_seconds'
CONF_RESET_SECONDS = 'reset_window_seconds'
CONF_EVENT = 'event'
CONF_EVENT_DATA = 'event_data'

KEY_REFERENCE = 'reference'

CONFIG_CONDITION_SCHEMA = vol.Schema(
    [conditions.CONFIG_CONDITION_BASE_SCHEMA],
    extra=vol.PREVENT_EXTRA)

SCHEMA = vol.Schema({
  vol.Required(CONF_TRIGGER_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_SUPPRESS_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,
  vol.Optional(CONF_DISABLE_CONDITION, default=[]): CONFIG_CONDITION_SCHEMA,

  vol.Required(CONF_EVENT): str,
  vol.Optional(CONF_EVENT_DATA, default={}): {},
  vol.Optional(CONF_WINDOW_SECONDS, default=120): vol.Range(min=0, max=300),
  vol.Optional(CONF_RESET_SECONDS, default=15*60): vol.Range(min=0, max=24*60*60),
}, extra=vol.ALLOW_EXTRA)


class CautiousNotifier(hass.Hass):
  def initialize(self):
    self._per_entity_last_supress = {}
    self._per_entity_last_trigger = {}
    self._last_overall_trigger = None

    self._config = SCHEMA(self.args)

    self._window_seconds = self._config.get(CONF_WINDOW_SECONDS)
    self._reset_seconds = self._config.get(CONF_RESET_SECONDS)
    self._event = self._config.get(CONF_EVENT)
    self._event_data = self._config.get(CONF_EVENT_DATA)

    self._suppress_condition = self._config.get(CONF_SUPPRESS_CONDITION)
    self._trigger_condition = self._config.get(CONF_TRIGGER_CONDITION)
    self._disable_condition = self._config.get(CONF_DISABLE_CONDITION)

    self._suppress_evaluation_times = [None] * len(self._suppress_condition)
    self._trigger_evaluation_times = [None] * len(self._trigger_condition)

    self._trigger_callback = None
    self._trigger_timeout = None

    for entity in conditions.extract_entities_from_condition(
        self._suppress_condition):
      self.listen_state(self._handle_suppress_state, entity)

    for entity in conditions.extract_entities_from_condition(
        self._trigger_condition):
      self.listen_state(self._handle_trigger_state, entity)

  def _handle_suppress_state(self, entity, attribute, old, new, kwargs):
    for i in range(0, len(self._suppress_condition)):
      condition = self._suppress_condition[i]
      if conditions.evaluate_condition(
          self, self.datetime().time(), [condition], triggers={entity: new}):
        self.log('Suppress condition evaluates true: %s (%s: %s->%s)' % (
            condition, entity, old, new))
        self._suppress_evaluation_times[i] = self.datetime()

  def _handle_trigger_state(self, entity, attribute, old, new, kwargs):
    for i in range(0, len(self._trigger_condition)):
      condition = self._trigger_condition[i]
      if conditions.evaluate_condition(
          self, self.datetime().time(), [condition], triggers={entity: new}):
        self.log('Trigger condition evaluates true: %s (%s: %s->%s)' % (
            condition, entity, old, new))
        self._trigger_evaluation_times[i] = self.datetime()

    if not self._trigger_callback:
      # Keep the time we expect the timeout to fire, and use that as the
      # reference time to avoid appdaemon scheduling delays breaking the time
      # comparisons.
      kwargs = {}
      kwargs[KEY_REFERENCE] = self.datetime() + datetime.timedelta(
          seconds=self._window_seconds)

      self._trigger_callback = self.run_in(
          self._trigger, self._window_seconds, **kwargs)

  def _trigger(self, kwargs=None):
    self._trigger_callback = None
    reference = kwargs[KEY_REFERENCE]

    for i in range(0, len(self._suppress_condition)):
      condition = self._suppress_condition[i]
      eval_time = self._suppress_evaluation_times[i]

      if eval_time:
        seconds_since_suppress = int((reference - eval_time).total_seconds())
        if seconds_since_suppress <= self._window_seconds:
          self.log('Suppress condition \'%s\' triggered too recently (%i secs '
                   'ago), skipping...' % (condition, seconds_since_suppress))
          return False

    for i in range(0, len(self._trigger_condition)):
      condition = self._trigger_condition[i]
      eval_time = self._trigger_evaluation_times[i]

      if not eval_time:
        self.log('Trigger condition \'%s\' has not yet triggered, skipping ...'
            % condition)
        return False
      seconds_since_trigger = int((reference - eval_time).total_seconds())
      if seconds_since_trigger > self._window_seconds:
        self.log('Trigger condition \'%s\' triggered too long ago (%i secs ago), '
                 'skipping...' % (condition, seconds_since_trigger))
        return False

    if self._disable_condition and conditions.evaluate_condition(
        self, self.datetime().time(), self._disable_condition):
      self.log('Disable condition \'%s\' evalutes true, skipping...' %
          self._disable_condition)
      return False

    if self._last_overall_trigger is not None:
      seconds_since_overall_trigger = int((self.datetime() -
          self._last_overall_trigger).total_seconds())
      if seconds_since_overall_trigger < self._reset_seconds:
        self.log('Overall trigger was too recent (%i secs ago), '
                 'skipping...' % seconds_since_overall_trigger)
        return False

    self._last_overall_trigger = self.datetime()
    self.fire_event(self._event, **self._event_data)
    self.log('Triggered! Fired event: \'%s\' with data \'%s\'' % (
        self._event, self._event_data))
