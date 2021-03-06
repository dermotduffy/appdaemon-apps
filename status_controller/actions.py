import copy
import datetime
import random
import time
import threading

import config as scc

# Expected workflow:
#
# - action = ActionFoo(args, **kwargs)
# - action.prepare()
# - action.action()
#
# action.is_finished() will be True when action is complete.

# A note on file format: For some unknown reason, if the media file is .wav
# format (vs mp3), there will be a stutter on play to a joined group (perhaps
# file-size/buffering?). This isn't a big deal for long clips, but for short
# sounds this can ruin the sound (e.g. a message chime). Converting to mp3
# avoids this issue.

SONOS_SERVICE_SNAPSHOT = 'sonos/snapshot'
SONOS_SERVICE_RESTORE = 'sonos/restore'
SONOS_SERVICE_JOIN = 'sonos/join'
SONOS_SERVICE_UNJOIN = 'sonos/unjoin'
SONOS_SERVICE_VOLUME_SET = 'media_player/volume_set'
SONOS_SERVICE_MEDIA_PLAY = 'media_player/play_media'
SONOS_SERVICE_MEDIA_STOP = 'media_player/media_stop'

class ActionBase(object):
  def __init__(self, app, complete_callback, **kwargs):
    # kill_action() may be called from a different thread.
    self._lock = threading.RLock()

    self._app = app
    self._complete_callback = complete_callback
    self._kwargs = kwargs
    self._is_finished = False
    self._priority = self._pop_argument(scc.CONF_PRIORITY)

  def get_priority(self):
    return self._priority

  def prepare(self):
    """Do whatever preparation is necessary. Get everything ready up to the
    point of visibility to the user."""
    pass

  def action(self):
    """Do the action that will visible to the user."""
    pass

  def complete_action(self, hard_kill_entities=None):
    """Complete any post-action steps."""
    with self._lock:
      if self._is_finished:
        return
      self._is_finished = True
    self._complete_callback(self)

  def _pop_argument(self, argument, default=None):
    with self._lock:
      return self._kwargs.pop(argument, default)

  def is_finished(self):
    with self._lock:
      return self._is_finished


class TimedActionBase(ActionBase):
  def __init__(self, app, complete_callback, **kwargs):
    super().__init__(app, complete_callback, **kwargs)
    self._complete_timer_handle = None
    self._action = self._pop_argument(scc.CONF_ACTION)
    self._length = self._pop_argument(scc.CONF_LENGTH)

  def complete_action(self, hard_kill_entities=None):
    """Complete any post-action steps."""
    with self._lock:
      if self._is_finished:
        return
      if self._complete_timer_handle is not None:
        self._cancel_timer(self._complete_timer_handle)
        self._complete_timer_handle = None
    super().complete_action(hard_kill_entities=hard_kill_entities)

  def _schedule_action_complete(self):
    with self._lock:
      if self._is_finished:
        return
      self._complete_timer_handle = self._app.run_in(
          self.complete_action,
          self._length)

  def _cancel_timer(self, timer_handle):
    self._app.cancel_timer(timer_handle)


class SonosAction(TimedActionBase):
  def __init__(self, app, complete_callback, entity_id, primary, **kwargs):
    super().__init__(app, complete_callback, **kwargs)

    self._entity_id = entity_id
    self._primary = primary
    self._volume = self._pop_argument(scc.CONF_SONOS_VOLUME)

    scc.log(self._app, self, 'Sonos entity: %s (Primary is: %s)' % (
        self._entity_id, self._primary))

  def _is_primary(self):
    return self._entity_id == self._primary

  def complete_action(self, hard_kill_entities=None):
    with self._lock:
      if self._is_finished:
        return
    if self._is_primary() and hard_kill_entities:
      # This does not currently implement per-entity stopping, which would
      # require breaking apart and re-assembling the groups. Instead, if any
      # entity needs to hard stop, the media is stopped on all entities.
      self._stop_media()
    super().complete_action(hard_kill_entities=hard_kill_entities)

  def prepare(self):
    super().prepare()

    self._unjoin()

    # Primary does not need to join itself.
    if not self._is_primary():
      self._join()

    self._set_volume()

  @classmethod
  def capture_global_sonos_state(cls, app):
    scc.log(app, cls, 'Saving global snapshot')
    app.call_service(SONOS_SERVICE_SNAPSHOT, entity_id='all')

  @classmethod
  def restore_global_sonos_state(cls, app):
    scc.log(app, cls, 'Restoring global snapshot')
    app.call_service(SONOS_SERVICE_RESTORE, entity_id='all')

  def _stop_media(self):
    with self._lock:
      if self._is_finished:
        return
    scc.log(self._app, self, 'Stopping play on: %s' % self._entity_id)
    self._app.call_service(
        SONOS_SERVICE_MEDIA_STOP,
        entity_id=self._entity_id)

  def _set_volume(self):
    if self._volume:
      with self._lock:
        if self._is_finished:
          return
      scc.log(self._app, self, 'Setting volume to %f for: %s' % (
          self._volume, self._entity_id))
      self._app.call_service(
          SONOS_SERVICE_VOLUME_SET,
          entity_id=self._entity_id,
          volume_level=self._volume)

  def _unjoin(self):
    with self._lock:
      if self._is_finished:
        return
    # Need to unjoin even if there's only 1 entity (as it may already be joined
    # to something else, we do not know).
    scc.log(self._app, self, 'Unjoining: %s' % self._entity_id)
    self._app.call_service(
        SONOS_SERVICE_UNJOIN,
        entity_id=self._entity_id)

  def _join(self):
    with self._lock:
      if self._is_finished:
        return
    scc.log(self._app, self, 'Joining \'%s\' to: %s' %
        (self._entity_id, self._primary))
    self._app.call_service(
        SONOS_SERVICE_JOIN,
        master=self._primary,
        entity_id=self._entity_id)

class SonosTTSAction(SonosAction):
  def __init__(self, app, complete_callback, entity_id, primary, **kwargs):
    super().__init__(app, complete_callback, entity_id, primary, **kwargs)

    self._message = self._pop_argument(scc.CONF_MESSAGE)
    self._tts_service = self._pop_argument(scc.CONF_SONOS_TTS_SERVICE)
    self._chime = self._pop_argument(scc.CONF_SONOS_CHIME)
    self._chime_length = self._pop_argument(scc.CONF_SONOS_CHIME_LENGTH)
    self._speak_timer_handle = None

  def complete_action(self, hard_kill_entities=None):
    with self._lock:
      if self._is_finished:
        return
      if self._speak_timer_handle:
        self._cancel_timer(self._speak_timer_handle)
        self._speak_timer_handle = None
    super().complete_action(hard_kill_entities=hard_kill_entities)

  def action(self):
    super().action()
    if not self._is_primary():
      self.complete_action()
      return

    if self._chime:
      self._action_chime()
    else:
      self._action_speak()

  def _action_chime(self):
    with self._lock:
      if self._is_finished:
        return

    scc.log(self._app, self, 'Chiming on %s: \'%s\'' % (
        self._entity_id, self._chime))
    self._app.call_service(
        SONOS_SERVICE_MEDIA_PLAY,
        entity_id=self._entity_id,
        media_content_id=self._chime,
        media_content_type='music')

    with self._lock:
      if not self._is_finished:
        self._speak_timer_handle = self._app.run_in(
            self._action_speak,
            self._chime_length)

  def _action_speak(self, kwargs=None):
    with self._lock:
      if self._is_finished:
        return
    scc.log(self._app, self, 'Speaking on %s: \'%s\'' % (
        self._entity_id, self._message))
    self._app.call_service(
        self._tts_service,
        entity_id=self._entity_id,
        message=self._message)
    self._schedule_action_complete()


class SonosPlayMediaAction(SonosAction):
  def __init__(self, app, complete_callback, entity_id, primary, **kwargs):
    super().__init__(app, complete_callback, entity_id, primary, **kwargs)
    self._media = self._pop_argument(scc.CONF_SONOS_MEDIA)

  def action(self):
    super().action()
    if not self._is_primary():
      return

    with self._lock:
      if self._is_finished:
        return

    scc.log(self._app, self, 'Playing media on %s: \'%s\'' % (
        self._entity_id, self._media))
    self._app.call_service(
        SONOS_SERVICE_MEDIA_PLAY,
        entity_id=self._entity_id,
        media_content_id=self._media,
        media_content_type='music')

    self._schedule_action_complete()


class LightActionBase(TimedActionBase):
  def __init__(self, app, complete_callback, entity_id,
               prior_state=None, **kwargs):
    super().__init__(app, complete_callback, **kwargs)
    self._finish_action = self._pop_argument(scc.CONF_FINISH_ACTION)
    self._entity_id = entity_id

    # The prior_state dict is also used as a general mechanism for passing
    # in the underlying entities, which may be different from the _entity_id.
    self._prior_state = prior_state

  def _sanitize_args(self, ref, **kwargs):
    output = {}
    for arg in kwargs:
      if arg in ref:
        output[arg] = kwargs[arg]
    return output

  def _toggle(self):
    scc.log(self._app, self, 'Toggling: %s (%s)' % (
        self._entity_id, self._kwargs))
    if self._app.get_state(self._entity_id) == 'on':
      self._turn_off()
    else:
      self._turn_on()

  def _turn_on_with_args(self, entity_id=None, **kwargs):
    with self._lock:
      if self._is_finished:
        return

    entity_id = entity_id or self._entity_id
    sanitized_args = self._sanitize_args(ref=scc.ARGS_FOR_TURN_ON, **kwargs)
    scc.log(self._app, self, 'Turning on: %s (%s)' % (entity_id, sanitized_args))
    self._app.turn_on(entity_id, **sanitized_args)

  def _turn_on(self):
    return self._turn_on_with_args(**self._kwargs)

  def _turn_off_with_args(self, entity_id=None, **kwargs):
    with self._lock:
      if self._is_finished:
        return

    entity_id = entity_id or self._entity_id
    sanitized_args = self._sanitize_args(ref=scc.ARGS_FOR_TURN_OFF, **kwargs)
    scc.log(self._app, self, 'Turning off: %s (%s)' % (entity_id, sanitized_args))
    self._app.turn_off(entity_id, **sanitized_args)

  def _turn_off(self):
    return self._turn_off_with_args(**self._kwargs)

  def complete_action(self, hard_kill_entities=None):
    with self._lock:
      if self._is_finished:
        return

    if not hard_kill_entities:
      if self._finish_action == scc.CONF_ACTION_LIGHT_TURN_ON:
        self._turn_on()
      elif self._finish_action == scc.CONF_ACTION_LIGHT_TURN_OFF:
        self._turn_off()
      elif self._finish_action == scc.CONF_ACTION_LIGHT_RESTORE:
        self._restore_state()
    else:
      with self._lock:
        for entity_id in self._prior_state:
          if entity_id in hard_kill_entities:
            continue
          if self._finish_action == scc.CONF_ACTION_LIGHT_TURN_ON:
            self._turn_on_with_args(entity_id=entity_id, **self._kwargs)
          elif self._finish_action == scc.CONF_ACTION_LIGHT_TURN_OFF:
            self._turn_off_with_args(entity_id=entity_id, **self._kwargs)
          elif self._finish_action == scc.CONF_ACTION_LIGHT_RESTORE:
            self._restore_state(entity_id=entity_id)
    super().complete_action(hard_kill_entities=hard_kill_entities)

  def _restore_state(self, entity_id=None):
    with self._lock:
      state = self._prior_state
      if not state:
        return

    if entity_id is not None:
      entities = [entity_id]
      scc.log(self._app, self, 'Reduced restore for %s' % entity_id)
    else:
      entities = state.keys()

    for entity_id in entities:
      scc.log(self._app, self, 'Restoring state for: %s (%s)' % (entity_id, state[entity_id]))
      if state[entity_id].get(scc.KEY_STATE) == 'on':
        self._turn_on_with_args(
            entity_id=entity_id,
            **self._sanitize_args(
                ref=scc.ATTR_ARGS_FOR_TURN_ON,
                **state[entity_id].get(scc.KEY_ATTRIBUTES)))
      elif state[entity_id].get(scc.KEY_STATE) == 'off':
        self._turn_off_with_args(
            entity_id=entity_id,
            **state[entity_id].get(scc.KEY_ATTRIBUTES))

  @classmethod
  def capture_state(cls, app, entity_id):
    prior_state = app.get_state(entity_id, attribute='all')
    scc.log(app, cls, 'Capturing state from \'%s\': %s' % (
        entity_id, prior_state))
    return prior_state


class SimpleLightAction(LightActionBase):
  def action(self):
    super().action()
    if self._action == scc.CONF_ACTION_LIGHT_TURN_ON:
      self._turn_on()
    elif self._action == scc.CONF_ACTION_LIGHT_TURN_OFF:
      self._turn_off()
    elif self._action == scc.CONF_ACTION_LIGHT_TOGGLE:
      self._toggle()

    self._schedule_action_complete()


class BreathingLightAction(LightActionBase):
  def __init__(self, app, complete_callback, entity_id,
               prior_state=None, **kwargs):
    super().__init__(app, complete_callback, entity_id, prior_state, **kwargs)

    self._beats_remaining = None

    breath_length = float(self._pop_argument(
        scc.CONF_BREATH_LENGTH, scc.DEFAULT_LIGHT_BREATH_LENGTH))

    self._beats_remaining = 2 * round(self._length / breath_length)
    self._beat_length = breath_length / 2

    # If it's on, give it an extra beat to toggle to off first, before
    # starting the right number of breathes.
    if self._app.get_state(entity_id) == 'on':
      self._beats_remaining += 1

    self._breathe_timer_handle = None

  def action(self):
    super().action()
    with self._lock:
      if self._is_finished:
        return

    self._breathe_timer_handle = self._app.run_every(
        self._breathe,
        'now',
        self._beat_length, **{})

  def complete_action(self, hard_kill_entities=None):
    with self._lock:
      if self._is_finished:
        return
      self._cancel_breathe_timer()
    super().complete_action(hard_kill_entities=hard_kill_entities)

  def _cancel_breathe_timer(self):
    with self._lock:
      if self._breathe_timer_handle is not None:
        self._cancel_timer(self._breathe_timer_handle)
        self._breathe_timer_handle = None

  def _breathe(self, kwargs):
    with self._lock:
      if self._is_finished:
        return
    if self._beats_remaining <= 0:
      self._cancel_breathe_timer()
      self.complete_action()
    else:
      self._toggle()
      self._beats_remaining -= 1


class ServiceAction(ActionBase):
  def __init__(self, app, complete_callback, **kwargs):
    super().__init__(app, complete_callback, **kwargs)

  def _call_service(self, service, **kwargs):
    scc.log(self._app, self, 'Calling: %s (%s)' % (service, kwargs))
    self._app.call_service(service, **kwargs)


class NotifyAction(ServiceAction):
  def __init__(self, app, complete_callback, **kwargs):
    super().__init__(app, complete_callback, **kwargs)

    self._notify_service = self._pop_argument(scc.CONF_SERVICE)

  def action(self):
    super().action()
    with self._lock:
      if self._is_finished:
        return
    self._call_service(self._notify_service, **self._kwargs)

class MQTTAction(ServiceAction):
  def __init__(self, app, complete_callback, **kwargs):
    super().__init__(app, complete_callback, **kwargs)

    self._notify_service = self._pop_argument(scc.CONF_SERVICE)
    self._topic = self._pop_argument(scc.CONF_ACTION_MQTT_TOPIC)
    self._payload = self._pop_argument(scc.CONF_ACTION_MQTT_PAYLOAD)

  def action(self):
    super().action()
    with self._lock:
      if self._is_finished:
        return
    kwargs = { scc.CONF_ACTION_MQTT_TOPIC: self._topic,
               scc.CONF_ACTION_MQTT_PAYLOAD: self._payload }
    self._call_service(self._notify_service, **kwargs)


LIGHT_ACTION_MAP = {
  scc.CONF_ACTION_LIGHT_TURN_ON: SimpleLightAction,
  scc.CONF_ACTION_LIGHT_TURN_OFF: SimpleLightAction,
  scc.CONF_ACTION_LIGHT_TOGGLE: SimpleLightAction,
  scc.CONF_ACTION_LIGHT_BREATHE: BreathingLightAction,
  scc.CONF_ACTION_INTERRUPT: None,
}

SONOS_ACTION_MAP = {
  scc.CONF_ACTION_SONOS_TTS: SonosTTSAction,
  scc.CONF_ACTION_SONOS_MEDIA_PLAY: SonosPlayMediaAction,
  scc.CONF_ACTION_INTERRUPT: None,
}
