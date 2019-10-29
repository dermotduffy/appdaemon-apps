import copy
import appdaemon.plugins.hass.hassapi as hass

DEFAULT_SERVICE = 'toggle'
DEFAULT_EVENT = 'zha_event'

KEY_FILTER = 'filter'
KEY_COMMAND = 'command'
KEY_ENTITY = 'entity'
KEY_ENTITY_ON = 'entity_on'
KEY_ENTITY_OFF = 'entity_off'
KEY_ENTITY_CHECK = 'entity_check'
KEY_SERVICE = 'service'
KEY_EVENT = 'event'
KEY_DEVICE_IEEE = 'device_ieee'
KEY_BRIGHTNESS_PCT = 'brightness_pct'

KEY_ARGS = 'args'

class Button(hass.Hass):
  def initialize(self):
    self.listen_event(
        self.handle_button_event,
        event=self.args.get(KEY_EVENT) or DEFAULT_EVENT,
        **self.args.get(KEY_FILTER, {}))
    self._rotate_on_indicies = {}
    self._last_command = None
    self._last_brightness_pct = None

  def _get_rotate_on_key(self, command, entities):
    return '%s/%s' % (command, ','.join(entities))

  def _to_list(self, data):
    if not data:
      return []
    elif not isinstance(data, list):
      return [data]
    return data

  def handle_button_event(self, event_name, data, kwargs):
    command = data.get(KEY_COMMAND)
    if command not in self.args:
      return

    command_args = self.args.get(command)
    if command_args is None:
      return

    if isinstance(command_args, dict):
      command_args = [command_args]

    for command_set in command_args:
      service = command_set.get(KEY_SERVICE) or DEFAULT_SERVICE
      entities = self._to_list(command_set.get(KEY_ENTITY))
      entities_on = self._to_list(command_set.get(KEY_ENTITY_ON))
      entities_off = self._to_list(command_set.get(KEY_ENTITY_OFF))
      entities_check = self._to_list(command_set.get(KEY_ENTITY_CHECK))
      service_args = command_set.get(KEY_ARGS) or {}

      if service in ['rotate_on']:
        rotate_entities = entities_on or entities
        key = self._get_rotate_on_key(command, rotate_entities)
        index = self._rotate_on_indicies.get(key, 0)
        self._rotate_on_indicies[key] = (index + 1) % len(rotate_entities)
        entities_on = [rotate_entities[index]]
        service = 'turn_on'

      if KEY_BRIGHTNESS_PCT in data:
        service_args[KEY_BRIGHTNESS_PCT] = data[KEY_BRIGHTNESS_PCT]

      if service in ['turn_on']:
        self.turn_on_entities(entities_on or entities, **service_args)
      elif service in ['turn_off']:
        self.turn_off_entities(entities_off or entities, **service_args)
      elif service in ['toggle']:
        self.toggle_entities(
            entities_on or entities,
            entities_off or entities,
            entities_check or entities,
            **service_args)

  def turn_on_entities(self, entities_on, **kwargs):
    for entity in entities_on:
      self.log('Turning on %s with args: %s' % (entity, kwargs))
      super().turn_on(entity, **kwargs)

  def turn_off_entities(self, entities_off, **kwargs):
    for entity in entities_off:
      self.log('Turning off %s' % entity)
      super().turn_off(entity)

  # Reimplement toggle due to:
  # https://github.com/home-assistant/home-assistant/issues/26808
  def toggle_entities(self, entities_on, entities_off,
                      entities_check, **kwargs):
    self.log('Toggle: Checking state of %s' % entities_check)
    if any([self.get_state(entity) == 'on' for entity in entities_check]):
      self.turn_off_entities(entities_off)
    else:
      self.turn_on_entities(entities_on, **kwargs)
