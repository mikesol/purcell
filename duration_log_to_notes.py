from sqlalchemy import Column, Integer, select, and_, case
from fractions import Fraction

def E(_13) :

  ####################################
  # assign duration_log to notes

  duration_logless_notes =\
    _13.last_outerjoin(
      _13.session.query(_13.Name.id,
                         _13.Duration.val),
                        _13.Name,
                        [_13.Duration, _13.Duration_log]).\
      filter(_13.Name.val == "note").\
      filter(_13.Duration_log.val == None).\
      with_labels().\
      subquery('duration_logless_notes')

  Duration_logless_notes = _13.reflect(duration_logless_notes, _13.Duration)

  duration_logless_ands =\
    [(and_(Duration_logless_notes.duration__val >= Fraction(2,1)**(x-2),
      Duration_logless_notes.duration__val < Fraction(2,1)**(x-1)), x)
    for x in range(-7,7)]

  return {
    "inserter" : [_13.insert(_13.Duration_log,
    select([duration_logless_notes.c.name_id,
            case(duration_logless_ands,
                 else_ = 147)]))]
  }
