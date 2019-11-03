import copy
import datetime
import logging
import operator
import os
import random
import time
import threading
import traceback

import appdaemon.plugins.hass.hassapi as hass
import voluptuous as vol

import config as scc
import actions

# A note on restoring the state pre-event:
#
# Sonos: Sonos groups are snapshot globally to avoid corner cases in grouping
# scenarios that may cause the wrong thing to be restored, and to avoid delay
# in snapshoting/restoring if it were done before & after each entity level event.
#
# Lights: State is captured centrally per entity, and restored in the action
# objects themselves. This deals better with multiple events that change the
# state of the same entity, as the correct 'at-the-start' state is restored
# when a series of events is finished.
#

class StatusControllerApp(hass.Hass):
  def initialize(self):
    config = scc.CONFIG_SCHEMA(self.args)
    self._status_controller = StatusController(self, config)
    self._status_controller.daemon = True
    self._status_controller.start()
    self.listen_event(
        self.handle_status_event,
        event=config.get(scc.CONF_EVENT_NAME))

  def handle_status_event(self, event_name, data, kwargs):
    self.log('Received event: %s (%s)' % (event_name, data))
    event = scc.EVENT_SCHEMA(data)
    self._status_controller.add(event)

class StatusController(threading.Thread):
  def __init__(self, app, config, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self._app = app
    self._config = config

    self._cv = threading.Condition()
    self._events = []

    # Entities that have an ongoing action.
    self._entity_to_action = {}

    # Capture state information.
    self._captured_global_sonos_state = False
    self._captured_light_state = {}

  def run(self):
    try:
      while True:
        self._run_controller_cycle()
    except Exception as e:
      # Funnel exceptions through the Appdaemon logger (otherwise we won't see
      # them at all)
      stack_trace = traceback.format_exc()
      self._app.error('%s%s%s' % (e, os.linesep, stack_trace), level="ERROR")

  def _is_sonos_action(self, action):
    return isinstance(action, actions.SonosAction)

  def _is_sonos_action_in_flight(self):
    for action in self._entity_to_action.values():
      if self._is_sonos_action(action):
        return True
    return False

  def _kill_actions_on_entities(self, entities):
    actions_to_kill = set()
    for entity in entities:
      if entity in self._entity_to_action:
        actions_to_kill.add(self._entity_to_action[entity])
    for action in actions_to_kill:
      action.kill_action()
    self._remove_actions(actions_to_kill)

  def _remove_actions(self, actions_to_remove):
    for entity in [key for key in self._entity_to_action
                   if self._entity_to_action[key] in actions_to_remove]:
      del(self._entity_to_action[entity])

  def _remove_finished_actions(self):
    finished_actions = set()
    for entity in self._entity_to_action:
      if self._entity_to_action[entity].is_finished():
        finished_actions.add(self._entity_to_action[entity])
    self._remove_actions(finished_actions)

  def _run_controller_cycle(self):
    with self._cv:
      self._app.log('Starting controller cycle, waiting...')
      self._cv.wait()
      self._app.log('...controller woken')

      # Clean up finished actions.
      self._remove_finished_actions()

      # Process new actions.
      for event_tpl in sorted(
          self._events,
          reverse=True,
          key = lambda x: x[0]):
        # Process the whole available event list, and only then add the
        # postponed events back (to avoid a high-priority event with a
        # contended entity from preventing uncontended events from being
        # processed.
        priority, force, event, outputs = event_tpl

        entities_in_outputs = self._get_entities_involved_in_outputs(outputs)
        if entities_in_outputs.intersection(set(self._entity_to_action)):
          if force:
            self._app.log('Found contended event. Force killing '
                          'actions using in-scope entities. Event: %s' % event)
            self._kill_actions_on_entities(entities_in_outputs)
          else:
            self._app.log('Found contended event. Postponing. Event: %s'
                % event)
            continue

        self._events.remove(event_tpl)
        self._process_single_event(event, outputs)

      # If there's a captured Sonos state, and there's no Sonos action in
      # flight (after new events have been added above), then it's time to
      # restore the state.
      if (self._captured_global_sonos_state and
          not self._is_sonos_action_in_flight()):
        actions.SonosAction.restore_global_sonos_state(self._app)
        self._captured_global_sonos_state = False

      # If there's a captured light state, and there's no light action for
      # that entity in flight (after new events have been added above), then it's
      # time to remove that saved state. It will be recaptured when needed.
      for entity_id in [key for key in self._captured_light_state
                        if key not in self._entity_to_action]:
        del(self._captured_light_state[entity_id])


  def add(self, event):
    priorities = set()
    force = False

    # Take the highest output priority, and use that as the event priority.
    outputs = self._get_matching_outputs(event)
    for output in outputs:
      settings = scc.get_event_arguments(
          self._config,
          event,
          output.get(scc.CONF_SETTINGS, None),
          scc.CONF_SETTINGS)
      if settings[scc.CONF_FORCE]:
        force = True

      for domain in (scc.CONF_SONOS, scc.CONF_LIGHT, scc.CONF_NOTIFY):
        if not domain in output:
          continue
        for entry in output[domain]:
          entry = scc.get_event_arguments(self._config, event, entry, domain)
          priorities.add(entry[scc.CONF_PRIORITY])

    if priorities:
      with self._cv:
        self._events.append((max(priorities), force, event, outputs))
        self._cv.notify()

  def _report_action_finished(self, action):
    with self._cv:
      self._cv.notify()

  def _get_matching_outputs(self, event) -> list:
    event_tags = event.get(scc.CONF_TAGS)
    matches = []
    current_time = self._app.datetime().time()

    for output in self._config.get(scc.CONF_OUTPUTS):
      if (scc.CONF_CONDITION in output and
          not self._evaluate_condition(event, output[scc.CONF_CONDITION])):
        continue
      matches.append(output)
    return matches

  def _evaluate_condition(self, event, condition_set, operator=scc.CONF_AND):
    value = None
    current_time = self._app.datetime().time()

    for condition in condition_set:
      for key in condition:
        if key in [scc.CONF_AND, scc.CONF_OR]:
          intermediate_value = self._evaluate_condition(
              event, condition[key], operator=key)
        elif key == scc.CONF_NOT:
          intermediate_value = not self._evaluate_condition(
              event, condition[key], operator=scc.CONF_AND)
        elif key == scc.CONF_AFTER:
          intermediate_value = condition[key] <= current_time
        elif key == scc.CONF_BEFORE:
          intermediate_value = current_time < condition[key]
        elif key == scc.CONF_BETWEEN:
          start, end = condition[key]
          if start < end:
            intermediate_value = start <= current_time < end
          else:
            intermediate_value = start <= current_time or current_time < end
        elif key == scc.CONF_TAG:
          intermediate_value = (condition[key] in event[scc.CONF_TAGS])
        else:
          intermediate_value = (self._app.get_state(key) == condition[key])

        if value is None:
          value = intermediate_value
        elif operator == scc.CONF_AND:
          value &= intermediate_value
        elif operator == scc.CONF_OR:
          value |= intermediate_value
        else:
          raise RuntimeError('Invalid operator: %s' % operator)

    if value is None:
      value = True
    return value

  def _process_single_event(self, event, outputs):
    executable_actions = []
    self._app.log('>> Creating actions: %s / %s' % (event, outputs))
    self._app.log('>>> Creating Sonos actions: %s' % event)
    executable_actions.extend(self._create_sonos_actions(event, outputs))
    self._app.log('>>> Creating Light actions: %s' % event)
    executable_actions.extend(self._create_light_actions(event, outputs))
    self._app.log('>>> Creating Notify event: %s' % event)
    executable_actions.extend(self._create_notify_actions(event, outputs))
    self._app.log('>> Finished creating actions: %s' % event)
    self._app.log('>> Total actions to execute: %i' % len(executable_actions))

    execution_groups = {}
    has_sonos_action = False

    for action in executable_actions:
      execution_groups.setdefault(action.get_priority(), []).append(action)

      if self._is_sonos_action(action):
        has_sonos_action = True
    self._app.log('To execute, groups: %s' % execution_groups)

    if has_sonos_action and not self._captured_global_sonos_state:
      # Capture global state, rather than doing it per-entity. As we cannot
      # read the group status, it's possible to end up with broken
      # configuration if we snapshot with only some entities (e.g. two
      # events, with different overlapping entity_ids will result in
      # capturing an an inappropriate intermediate state).
      actions.SonosAction.capture_global_sonos_state(self._app)
      self._captured_global_sonos_state = True

    for priority_key in sorted(execution_groups, reverse=True):
      self._app.log('>>> Executing actions with priority: %i' % priority_key)
      for action_obj in execution_groups[priority_key]:
        action_obj.prepare()
      for action_obj in execution_groups[priority_key]:
        action_obj.action()

    self._app.log('>> Finished with single event: %s' % event)

  def _get_entities_involved_in_outputs(self, outputs) -> set:
    entities = set()
    for output in outputs:
      for domain in [scc.CONF_SONOS, scc.CONF_LIGHT]:
        if domain in output:
          for entity_set in output[domain]:
            for entity in entity_set.get(scc.CONF_ENTITIES):
              entities.add(entity[scc.CONF_ENTITY_ID])
    return entities

  def _get_sonos_primary(self, group_entities):
    return sorted(group_entities, reverse=True,
                  # x is (entity, arguments)
                  # x[1] are the arguments
                  # x[1][scc.CONF_PRIORITY] is the priority
                  # ...)[0] is the highest priority pair of (entity, arguments)
                  # ...)[0][0] is the priority of the highest priority pair.
                  key=lambda x: x[1][scc.CONF_PRIORITY])[0][0]

  def _create_sonos_actions(self, event, outputs):
    visited_entity_ids = []
    sonos_groups = {}

    # Get all the sonos players with the same group key.
    for output in outputs:
      if scc.CONF_SONOS in output:
        for sonos in output.get(scc.CONF_SONOS):
          arguments = scc.get_event_arguments(self._config, event, sonos, scc.CONF_SONOS)
          filtered_args = self._filter_sonos_args(arguments)
          tmp = filtered_args.items()
          group_key = frozenset(filtered_args.items())

          for entity in arguments.get(scc.CONF_ENTITIES):
            entity_id = entity[scc.CONF_ENTITY_ID]

            # Only invoke the 1st action that involves this entity in this event.
            if entity_id in visited_entity_ids:
              continue
            visited_entity_ids.append(entity_id)

            sonos_groups.setdefault(group_key, []).append((entity_id, arguments))

    sonos_actions = []

    for group in sonos_groups:
      primary = self._get_sonos_primary(sonos_groups[group])

      for entity_id, arguments in sonos_groups[group]:
        action = arguments.get(scc.CONF_ACTION)
        action_cls = actions.SONOS_ACTION_MAP[action]
        if not action_cls:
          continue
        action_obj = action_cls(self._app, self._report_action_finished,
                                entity_id, primary, **arguments)
        self._entity_to_action[entity_id] = action_obj
        sonos_actions.append(action_obj)

    return sonos_actions

  def _filter_sonos_args(self, arguments):
    return {
        key: arguments[key]
        for key in arguments
        if key not in scc.SONOS_GROUP_IGNORE_KEYS}

  def _create_light_actions(self, event, outputs):
    visited_entity_ids = []
    light_actions = []

    for output in outputs:
      if scc.CONF_LIGHT in output:
        for light in output.get(scc.CONF_LIGHT):
          arguments = scc.get_event_arguments(
              self._config, event, light, scc.CONF_LIGHT)
          for entity in arguments.pop(scc.CONF_ENTITIES):
            entity_id = entity[scc.CONF_ENTITY_ID]

            # Only invoke the 1st action that involves this entity in this event.
            if entity_id in visited_entity_ids:
              continue
            visited_entity_ids.append(entity_id)

            action_cls = actions.LIGHT_ACTION_MAP[arguments.get(scc.CONF_ACTION)]
            if not action_cls:
              continue

            prior_state = self._captured_light_state.get(entity_id, {})

            if not prior_state:
              if scc.CONF_UNDERLYING_ENTITY_IDS in entity:
                underlying_entity_ids = entity[scc.CONF_UNDERLYING_ENTITY_IDS]
              else:
                underlying_entity_ids = [entity_id]

              for underlying_entity_id in underlying_entity_ids:
                entity_state = actions.LightActionBase.capture_state(
                    self._app, underlying_entity_id)
                prior_state[underlying_entity_id] = entity_state
              self._captured_light_state[entity_id] = prior_state

            action = action_cls(
                self._app, self._report_action_finished,
                entity_id, prior_state, **arguments)

            self._entity_to_action[entity_id] = action
            light_actions.append(action)

    return light_actions

  def _create_notify_actions(self, event, outputs):
    notify_actions = []
    for output in outputs:
      if scc.CONF_NOTIFY in output:
        for notify in output.get(scc.CONF_NOTIFY):
          arguments = scc.get_event_arguments(
              self._config, event, notify, scc.CONF_NOTIFY)
          notify_actions.append(actions.NotifyAction(
              self._app, self._report_action_finished, **arguments))
    return notify_actions
