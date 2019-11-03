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
    self._app = app
    self._complete_callback = complete_callback
    self._kwargs = kwargs
    self._is_finished = False

    self._priority = self._pop_argument(scc.CONF_PRIORITY)

    # kill_action() may be called from a different thread, so lock
    # protect the underlying _complete_action and _is_finished as appropriate.
    self._lock = threading.RLock()

  def get_priority(self):
    return self._priority

  def prepare(self):
    """Do whatever preparation is necessary. Get everything ready up to the
    point of visibility to the user."""
    pass

  def action(self):
    """Do the action that will visible to the user."""
    pass

  def kill_action(self):
    self._complete_action(force=True)

  def _complete_action(self, force=False):
    """Complete any post-action steps."""
    with self._lock:
      if self._is_finished:
        return
      self._is_finished = True
    self._complete_callback(self)

  def _pop_argument(self, argument, default=None):
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

  def _complete_action(self, force=False):
    """Complete any post-action steps."""
    with self._lock:
      if self._is_finished:
        return
      if self._complete_timer_handle is not None:
        self._cancel_timer(self._complete_timer_handle)
        self._complete_timer_handle = None
    super()._complete_action(force=force)

  def _schedule_action_complete(self):
    self._complete_timer_handle = self._app.run_in(
        self._complete_action,
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

  def _complete_action(self, force=False):
    with self._lock:
      if self._is_finished:
        return
    if self._is_primary() and force:
      self._stop_media()
    super()._complete_action(force=force)

  def prepare(self):
    super().prepare()
    self._unjoin()

    if not self._is_primary():
      # Primary does not need to join itself.
      self._join()

    self._set_volume()

  @classmethod
  def capture_global_sonos_state(cls, app):
    scc.log(app, cls, 'Saving global snapshot')
    app.call_service(SONOS_SERVICE_SNAPSHOT)

  @classmethod
  def restore_global_sonos_state(cls, app):
    scc.log(app, cls, 'Restoring global snapshot')
    app.call_service(SONOS_SERVICE_RESTORE)

  def _stop_media(self):
    scc.log(self._app, self, 'Stopping play on: %s' % self._entity_id)
    self._app.call_service(
        SONOS_SERVICE_MEDIA_STOP,
        entity_id=self._entity_id)

  def _set_volume(self):
    if self._volume:
      scc.log(self._app, self, 'Setting volume to %f for: %s' % (
          self._volume, self._entity_id))
      self._app.call_service(
          SONOS_SERVICE_VOLUME_SET,
          entity_id=self._entity_id,
          volume_level=self._volume)

  def _unjoin(self):
    # Need to unjoin even if there's only 1 entity (as it may already be joined
    # to something else, we do not know).
    scc.log(self._app, self, 'Unjoining: %s' % self._entity_id)
    self._app.call_service(
        SONOS_SERVICE_UNJOIN,
        entity_id=self._entity_id)

  def _join(self):
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

  def _complete_action(self, force=False):
    with self._lock:
      if self._is_finished:
        return
      if self._speak_timer_handle:
        self._cancel_timer(self._speak_timer_handle)
        self._speak_timer_handle = None
    super()._complete_action(force=force)

  def action(self):
    super().action()
    if not self._is_primary():
      self._complete_action()
      return

    if self._chime:
      self._action_chime()
      self._speak_timer_handle = self._app.run_in(
        self._action_speak,
        self._chime_length)
    else:
      self._action_speak()

  def _action_chime(self):
    scc.log(self._app, self, 'Chiming on %s: \'%s\'' % (
        self._entity_id, self._chime))
    self._app.call_service(
        SONOS_SERVICE_MEDIA_PLAY,
        entity_id=self._entity_id,
        media_content_id=self._chime,
        media_content_type='music')

  def _action_speak(self, kwargs=None):
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

  def _turn_on_with_args(self, **kwargs):
    scc.log(self._app, self, 'Turning on: %s (%s)' % (self._entity_id, kwargs))
    self._app.turn_on(
        self._entity_id,
        **(self._sanitize_args(ref=scc.ARGS_FOR_TURN_ON, **kwargs)))

  def _turn_on(self):
    return self._turn_on_with_args(**self._kwargs)

  def _turn_off_with_args(self, **kwargs):
    scc.log(self._app, self, 'Turning off: %s (%s)' % (self._entity_id, kwargs))
    self._app.turn_off(
        self._entity_id,
        **(self._sanitize_args(ref=scc.ARGS_FOR_TURN_OFF, **kwargs)))

  def _turn_off(self):
    return self._turn_off_with_args(**self._kwargs)

  def _complete_action(self, force=False):
    with self._lock:
      if self._is_finished:
        return
    if not force:
      self._do_finish_action()
    super()._complete_action(force=force)

  def _do_finish_action(self):
    if self._finish_action == scc.CONF_ACTION_LIGHT_TURN_ON:
      self._turn_on()
    elif self._finish_action == scc.CONF_ACTION_LIGHT_TURN_OFF:
      self._turn_off()
    elif self._finish_action == scc.CONF_ACTION_LIGHT_RESTORE:
      self._restore_state()
    else:
      super()._do_finish_action()

  def _restore_state(self):
    with self._lock:
      state = self._prior_state
      if state is None:
        return
      self._prior_state = None

    if state.get(scc.KEY_STATE) == 'on':
      self._turn_on_with_args(
          **self._sanitize_args(
              ref=scc.ATTR_ARGS_FOR_TURN_ON,
              **state.get(scc.KEY_ATTRIBUTES)))
    elif state.get(scc.KEY_STATE) == 'off':
      self._turn_off_with_args(**state.get(scc.KEY_ATTRIBUTES))

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
    self._breathe_timer_handle = self._app.run_every(
        self._breathe,
        self._app.datetime(),
        self._beat_length, **{})

  def _complete_action(self, force=False):
    with self._lock:
      if self._is_finished:
        return
      self._cancel_breathe_timer()
    super()._complete_action(force=force)

  def _cancel_breathe_timer(self):
    if self._breathe_timer_handle is not None:
      self._cancel_timer(self._breathe_timer_handle)
      self._breathe_timer_handle = None

  def _breathe(self, kwargs):
    if self._beats_remaining <= 0:
      self._cancel_breathe_timer()
      self._complete_action()
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
    self._message = self._pop_argument(scc.CONF_MESSAGE)
    self._title = self._pop_argument(scc.CONF_TITLE, None)

  def action(self):
    super().action()
    kwargs = { 'message': self._message }
    if self._title is not None:
      kwargs['title'] = self._title

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
