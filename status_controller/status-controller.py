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

  def _is_sonos_action_in_flight(self):
    for action in self._entity_to_action.values():
      if isinstance(action, actions.SonosAction):
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
        priority, settings, event, outputs = event_tpl

        entities_in_outputs = self._get_entities_involved_in_outputs(outputs)
        if entities_in_outputs.intersection(set(self._entity_to_action)):
          if settings[scc.CONF_FORCE]:
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
    priorities = []
    outputs = self._get_matching_outputs(event)
    for output in outputs:
      settings = scc.get_event_arguments(
          self._config,
          event,
          output.get(scc.CONF_SETTINGS, None),
          scc.CONF_SETTINGS)
      priorities.append(settings[scc.CONF_PRIORITY])
    if priorities:
      with self._cv:
        self._events.append((max(priorities), settings, event, outputs))
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
    self._app.log('>> Processing single event: %s / %s' % (event, outputs))
    self._app.log('>>> Processing Sonos event: %s' % event)
    self._create_sonos_actions(event, outputs)
    self._app.log('>>> Processing Light event: %s' % event)
    self._create_light_actions(event, outputs)
    self._app.log('>>> Processing Notify event: %s' % event)
    self._create_notify_actions(event, outputs)
    self._app.log('>> Finished with single event: %s' % event)

  def _get_entities_involved_in_outputs(self, outputs) -> set:
    entities = set()
    for output in outputs:
      for domain in [scc.CONF_SONOS, scc.CONF_LIGHT]:
        if domain in output:
          for entity_set in output[domain]:
            for entity_id in entity_set.get(scc.CONF_ENTITIES):
              entities.add(entity_id)
    return entities

  def _create_sonos_actions(self, event, outputs):
    visited_entity_ids = []
    sonos_groups_unique_args = {}
    sonos_groups_entity_order = {}

    # Get all the sonos players with the same group key.
    for output in outputs:
      if scc.CONF_SONOS in output:
        for sonos in output.get(scc.CONF_SONOS):
          arguments = scc.get_event_arguments(self._config, event, sonos, scc.CONF_SONOS)
          for entity_id in arguments.get(scc.CONF_ENTITIES):
            # Only invoke the 1st action that involves this entity in this event.
            if entity_id in visited_entity_ids:
              continue
            visited_entity_ids.append(entity_id)

            common_args = self._get_sonos_common_args(arguments)
            unique_args = self._get_sonos_unique_args(arguments)
            group_key = frozenset(common_args.items())
            sonos_groups_unique_args.setdefault(
                group_key, {})[entity_id] = unique_args
            sonos_groups_entity_order.setdefault(
                group_key, []).append(entity_id)

    # Create action objects.
    actions_join_first = []
    actions_play_first = []

    for group in sonos_groups_unique_args:
      common_args = dict(group)
      unique_args = sonos_groups_unique_args[group]
      entity_order = sonos_groups_entity_order[group]

      action = common_args.get(scc.CONF_ACTION)
      action_cls = actions.SONOS_ACTION_MAP[action]
      if not action_cls:
        continue
      action_obj = action_cls(self._app, self._report_action_finished,
                              entity_order, unique_args, **common_args)

      # Determine if we want to play or join first.
      for entity_id in entity_order:
        if scc.CONF_SONOS_PLAY_FIRST in unique_args[entity_id]:
          actions_play_first.append(action_obj)
          break
      else:
        actions_join_first.append(action_obj)

      for entity in entity_order:
        self._entity_to_action[entity] = action_obj

    if actions_play_first or actions_join_first:
      if not self._captured_global_sonos_state:
        # Capture global state, rather than doing it per-entity. As we cannot
        # read the group status, it's possible to end up with broken
        # configuration if we snapshot with only some entities (e.g. two
        # events, with different overlapping entity_ids will result in
        # capturing an ainappropriate intermediate state).
        actions.SonosAction.capture_global_sonos_state(self._app)
        self._captured_global_sonos_state = True

      self._raw_execute_sonos_actions(actions_play_first, actions_join_first)

  def _raw_execute_sonos_actions(self, actions_play_first, actions_join_first):
    # There are two kinds of actions, those with play_first and those without.
    # Those with play_first will start playing on the master device first, then
    # join the others to it. This is useful in low-latency instances where some
    # sound is desired immediately. join_first on the other hand will join all
    # devices first, causing play back to be perfectly in sync, but at a cost
    # of extra latency.

    if actions_play_first:
      for action in actions_play_first:
        action.prepare(primary_only=True)
      for action in actions_play_first:
        action.action()
      for action in actions_play_first:
        action.prepare(secondaries_only=True)

    if actions_join_first:
      # Do all the preparation first (join/unjoin, etc) so that the actions are
      # as close to sync'd as possible.
      self._raw_execute_generic_actions(actions_join_first)

  def _get_sonos_common_args(self, arguments):
    return {
        key: arguments[key]
        for key in arguments
        if key not in scc.SONOS_GROUP_IGNORE_KEYS}

  def _get_sonos_unique_args(self, arguments):
    return {
        key: arguments[key]
        for key in arguments
        if key in scc.SONOS_GROUP_IGNORE_KEYS}

  def _create_light_actions(self, event, outputs):
    visited_entity_ids = []
    light_actions = []

    for output in outputs:
      if scc.CONF_LIGHT in output:
        for light in output.get(scc.CONF_LIGHT):
          arguments = scc.get_event_arguments(
              self._config, event, light, scc.CONF_LIGHT)
          for entity_id in arguments.pop(scc.CONF_ENTITIES):
            # Only invoke the 1st action that involves this entity in this event.
            if entity_id in visited_entity_ids:
              continue
            visited_entity_ids.append(entity_id)

            action_cls = actions.LIGHT_ACTION_MAP[arguments.get(scc.CONF_ACTION)]
            if not action_cls:
              continue

            prior_state = self._captured_light_state.get(entity_id, None)
            if not prior_state:
              prior_state =  actions.LightActionBase.capture_state(
                  self._app, entity_id)
              self._captured_light_state[entity_id] = prior_state

            action = action_cls(
                self._app, self._report_action_finished,
                entity_id, prior_state, **arguments)

            self._entity_to_action[entity_id] = action
            light_actions.append(action)

    if light_actions:
       self._raw_execute_generic_actions(light_actions)

  def _raw_execute_generic_actions(self, generic_actions):
    for action in generic_actions:
      action.prepare()
    for action in generic_actions:
      action.action()

  def _create_notify_actions(self, event, outputs):
    notify_actions = []
    for output in outputs:
      if scc.CONF_NOTIFY in output:
        for notify in output.get(scc.CONF_NOTIFY):
          arguments = scc.get_event_arguments(
              self._config, event, notify, scc.CONF_NOTIFY)
          notify_actions.append(actions.NotifyAction(
              self._app, self._report_action_finished, **arguments))
    if notify_actions:
      self._raw_execute_generic_actions(notify_actions)
