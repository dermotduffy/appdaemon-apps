import datetime
import voluptuous as vol

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

def ConstrainTime(fmt='%H:%M:%S'):
  return lambda v: datetime.datetime.strptime(v, fmt).time()

def ConstrainTimeRange(fmt='%H:%M:%S'):
  return lambda v: tuple(
      datetime.datetime.strptime(t, fmt).time() for t in v.split('-'))

CONFIG_CONDITION_BASE_SCHEMA = {
  vol.Optional(CONF_OR): vol.Self,
  vol.Optional(CONF_AND): vol.Self,
  vol.Optional(CONF_NOT): vol.Self,
  vol.Optional(CONF_AFTER): ConstrainTime(),
  vol.Optional(CONF_BEFORE): ConstrainTime(),
  vol.Optional(CONF_BETWEEN): ConstrainTimeRange(),
  vol.Optional(CONF_KIND, default=DEFAULT_KIND): vol.In(VALID_KINDS),
  str: str,
}

def evaluator_AND_OR(app, current_time, key, condition, triggers,
                     evaluators, default_evaluator, operator, kind):
  return evaluate_condition(
      app, current_time, condition, triggers,
      evaluators, default_evaluator, key)

def evaluator_NOT(app, current_time, key, condition, triggers,
                  evaluators, default_evaluator, operator, kind):
  return not evaluate_condition(
      app, current_time, condition, triggers,
      evaluators, default_evaluator, operator)

def evaluator_BEFORE(app, current_time, key, condition, triggers,
                     evaluators, default_evaluator, operator, kind):
  return current_time < condition

def evaluator_AFTER(app, current_time, key, condition, triggers,
                    evaluators, default_evaluator, operator, kind):
  return current_time >= condition

def evaluator_BETWEEN(app, current_time, key, condition, triggers,
                      evaluators, default_evaluator, operator, kind):
  start, end = condition
  if start < end:
    return start <= current_time < end
  else:
    return start <= current_time or current_time < end

def evaluator_DEFAULT(app, current_time, key, condition, triggers,
                      evaluators, default_evaluator, operator, kind):
  for numeric_operator in ('<=', '<', '>=', '>'):
    if condition.startswith(numeric_operator):
      rval = float(condition[len(numeric_operator):])
      if kind == CONF_KIND_TRIGGER:
        if key not in triggers:
          return False
        lval = float(triggers[key])
      else:
        lval = float(app.get_state(key))

      if numeric_operator == '<=':
        return lval <= rval
      elif numeric_operator == '<':
        return lval < rval
      elif numeric_operator == '>':
        return lval > rval
      else: # >=
        return lval >= rval
  if kind == CONF_KIND_TRIGGER:
    return key in triggers and triggers[key] == condition
  else:
    return app.get_state(key) == condition

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
                       kind=CONF_KIND_STATE):
  value = None
  for condition in condition_set:
    kind = condition[CONF_KIND]
    for key in {k:v for (k, v) in condition.items() if k != CONF_KIND}:
      if key in evaluators:
        intermediate_value = evaluators[key](
            app, current_time, key, condition[key], triggers,
            evaluators, default_evaluator, operator, kind)
      else:
        intermediate_value = default_evaluator(
            app, current_time, key, condition[key], triggers,
            evaluators, default_evaluator, operator, kind)

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
