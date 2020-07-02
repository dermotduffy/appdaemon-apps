import datetime
import voluptuous as vol

KEY_DEBUG = 'debug'

CONF_OR = 'or'
CONF_AND = 'and'
CONF_NOT = 'not'
CONF_AFTER = 'after'
CONF_BEFORE = 'before'
CONF_BETWEEN = 'between'
CONF_KIND = 'kind'
CONF_KIND_STATE = 'state'
CONF_KIND_TRIGGER = 'trigger'

DEFAULT_KIND = CONF_KIND_STATE
VALID_KINDS = [CONF_KIND_STATE, CONF_KIND_TRIGGER]

def ConstrainTimeRange(fmt='%H:%M:%S'):
  return lambda v: tuple([
      v.split('->')[0].strip(),
      v.split('->')[1].strip()])

CONFIG_CONDITION_BASE_SCHEMA = {
  vol.Optional(CONF_OR): vol.Self,
  vol.Optional(CONF_AND): vol.Self,
  vol.Optional(CONF_NOT): vol.Self,
  vol.Optional(CONF_AFTER): str,
  vol.Optional(CONF_BEFORE): str,
  vol.Optional(CONF_BETWEEN): ConstrainTimeRange(),
  vol.Optional(CONF_KIND, default=DEFAULT_KIND): vol.In(VALID_KINDS),
  str: str,
}

def evaluator_AND_OR(app, current_time, key, condition, triggers,
                     evaluators, default_evaluator, operator, kind, **kwargs):
  return evaluate_condition(
      app, current_time, condition, triggers,
      evaluators, default_evaluator, key, kind, **kwargs)

def evaluator_NOT(app, current_time, key, condition, triggers,
                  evaluators, default_evaluator, operator, kind, **kwargs):
  return not evaluate_condition(
      app, current_time, condition, triggers,
      evaluators, default_evaluator, operator, kind, **kwargs)

def _parse_time(app, condition, key):
  try:
    return app.parse_time(condition)
  except ValueError:
    app.log("Warning: Could not convert '%s' to datetime in condition "
             "evaluation. Configuration is incorrect. Condition will "
             "always evaluate false: key='%s'" % (condition, key))
  return None

def evaluator_BEFORE(app, current_time, key, condition, triggers,
                     evaluators, default_evaluator, operator, kind, **kwargs):
  val = _parse_time(app, condition, key)
  if val is None:
    return False
  return current_time < val


def evaluator_AFTER(app, current_time, key, condition, triggers,
                    evaluators, default_evaluator, operator, kind, **kwargs):
  val = _parse_time(app, condition, key)
  if val is None:
    return False
  return current_time >= val

def evaluator_BETWEEN(app, current_time, key, condition, triggers,
                      evaluators, default_evaluator, operator, kind, **kwargs):
  start = _parse_time(app, condition[0], key)
  end = _parse_time(app, condition[1], key)

  if start is None or end is None:
    return False
  if start < end:
    return start <= current_time < end
  else:
    return start <= current_time or current_time < end

def evaluator_DEFAULT(app, current_time, key, condition, triggers,
                      evaluators, default_evaluator, operator, kind, **kwargs):
  for numeric_operator in ('<=', '<', '>=', '>'):
    if condition.startswith(numeric_operator):
      if kind == CONF_KIND_TRIGGER:
        if key not in triggers:
          return False
        lval_str = triggers[key]
      else:
        lval_str = app.get_state(key)
      rval_str =  condition[len(numeric_operator):]

      try:
        rval = float(rval_str)
      except ValueError:
        app.log("Warning: Could not convert '%s' to rval float in condition "
                "evaluation. Configuration is incorrect. Condition will "
                "always evaluate false: key='%s', condition='%s'" % (
            (rval_str, key, condition)))
        return False

      try:
        lval = float(lval_str)
      except ValueError:
        app.log("Warning: Could not convert '%s' to lval float in condition "
                "evaluation. Condition will always evaluate false: "
                "key='%s', condition='%s'" % (lval_str, key, condition))
        return False

      if numeric_operator == '<=':
        return lval <= rval
      elif numeric_operator == '<':
        return lval < rval
      elif numeric_operator == '>':
        return lval > rval
      else: # >=
        return lval >= rval
  if kind == CONF_KIND_TRIGGER:
    return key in triggers and (triggers[key] == condition or condition == '*')
  else:
    return app.get_state(key) == condition or condition == '*'

BASE_EVALUATORS = {
  CONF_AND: evaluator_AND_OR,
  CONF_OR: evaluator_AND_OR,
  CONF_NOT: evaluator_NOT,
  CONF_AFTER: evaluator_AFTER,
  CONF_BEFORE: evaluator_BEFORE,
  CONF_BETWEEN: evaluator_BETWEEN,
}

def evaluate_condition(app, current_time, condition_set,
                       triggers=None,
                       evaluators=BASE_EVALUATORS,
                       default_evaluator=evaluator_DEFAULT, operator=CONF_AND,
                       kind=CONF_KIND_STATE,
                       **kwargs):
  value = None
  for condition in condition_set:
    kind = condition[CONF_KIND]
    for key in {k:v for (k, v) in condition.items() if k != CONF_KIND}:
      if key in evaluators:
        intermediate_value = evaluators[key](
            app, current_time, key, condition[key], triggers,
            evaluators, default_evaluator, operator, kind, **kwargs)
      else:
        intermediate_value = default_evaluator(
            app, current_time, key, condition[key], triggers,
            evaluators, default_evaluator, operator, kind, **kwargs)

      if KEY_DEBUG in kwargs and kwargs[DEBUG] == True:
        app.log('----- Evaluator: (%s:%s:%s) -> %s' % (
            kind, key, condition[key], intermediate_value))

      if value is None:
        value = intermediate_value
      elif operator == CONF_AND:
        value &= intermediate_value
      elif operator == CONF_OR:
        value |= intermediate_value
      else:
        raise RuntimeError('Invalid operator: %s' % operator)

  if value is None:
    value = True
  return value

def extractor_AND_OR_NOT(key, condition, extractors, default_extractor):
  return extract_entities_from_condition(
      condition, extractors, default_extractor)

def extractor_NULL(key, condition, extractors, default_extractor):
  return []

def extractor_DEFAULT(key, condition, extractors, default_extractor):
  return [key]

BASE_EXTRACTORS = {
  CONF_AND: extractor_AND_OR_NOT,
  CONF_OR: extractor_AND_OR_NOT,
  CONF_NOT: extractor_AND_OR_NOT,
  CONF_AFTER: extractor_NULL,
  CONF_BEFORE: extractor_NULL,
  CONF_BETWEEN: extractor_NULL,
}

def extract_entities_from_condition(
    condition_set, extractors=BASE_EXTRACTORS,
    default_extractor=extractor_DEFAULT):
  entities = []
  for condition in condition_set:
    for key in {k:v for (k, v) in condition.items() if k != CONF_KIND}:
      if key in extractors:
        entities.extend(extractors[key](
            key, condition[key], extractors, default_extractor))
      else:
        entities.extend(default_extractor(
            key, condition[key], extractors, default_extractor))
  return entities
