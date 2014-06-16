from sqlalchemy import Column, Integer, Float, and_, delete

def E(_13, needle, first = True) :

  ####################################
  # assign first needle to all staves

  time_k = _13.Onset if first else _13.End
  col_label = 'onset_min' if first else 'end_max'
  col_long_label = 'onset__min' if first else 'end__max'
  prop = 'min' if first else 'max'

  first_events_on_staffs =\
    _13.join(_13.session.query(_13.Score._a[0].val,
                           _13.Staff._a[0].val,
                           time_k._a[0].val,
                           getattr(time_k._a[0], prop).label(col_label)),
             _13.Score._a[0], [_13.Staff._a[0],
                               time_k._a[0]]).\
      group_by(_13.Score._a[0].val, _13.Staff._a[0].val).\
      order_by(col_label).\
      with_labels().\
      subquery(name="first_events_on_staffs")

  first_needles_on_staffs =\
    _13.join(_13.session.query(_13.Score._a[1].val,
                           _13.Staff._a[1].val,
                           time_k._a[1].val,
                           getattr(time_k._a[1], prop).label(col_label),
                           _13.Name._a[1]),
             _13.Score._a[1], [_13.Staff._a[1],
                               time_k._a[1],
                               _13.Name._a[1]]).\
      filter(_13.Name._a[1].val == needle).\
      group_by(_13.Score._a[1].val, _13.Staff._a[1].val).\
      order_by(col_label).\
      with_labels().\
      subquery(name="first_celfs_on_staffs")

  instr = [_13.Score, _13.Staff, time_k, Column(col_long_label, Float)]

  First_events_on_staffs = _13.map(first_events_on_staffs,
                                   "First_events_on_staffs",
                                   *instr)
  First_needles_on_staffs = _13.map(first_needles_on_staffs,
                                  "First_needles_on_staffs",
                                  *(instr+[_13.Name]))

  joined_firsts_to_needles = _13.session.query(
                             First_events_on_staffs.score__val,
                             First_events_on_staffs.staff__val,
                             First_events_on_staffs.onset__val,
                             "'{0}'".format(needle)).outerjoin(
    First_needles_on_staffs,
      and_(First_events_on_staffs.score__val ==\
             First_needles_on_staffs.score__val,
           First_events_on_staffs.staff__val ==\
             First_needles_on_staffs.staff__val,
           First_events_on_staffs.onset__val ==\
             First_needles_on_staffs.onset__val)).\
    filter(First_needles_on_staffs.name__val == None).\
    subquery(name="joined_firsts_to_needles")

  First_needle_engraver = _13.map(_13.metadata, "First_{0}_engraver".format(needle),
     Column('id', Integer, primary_key=True),
     _13.Score, _13.Staff, time_k, _13.Name, Column('pointer', Integer))

  needle_insert = _13.t(First_needle_engraver).insert().from_select(
       _13.names(_13.Score, _13.Staff, time_k, _13.Name),
       joined_firsts_to_needles)

  deleter = [delete(_13.t(First_needle_engraver))]

  return {
    "tables" : [_13.t(First_needle_engraver)],
    "inserter" : [needle_insert],
    "pointer" : _13.make_pointer_assigner(First_needle_engraver),
    "updater" : _13.make_updater(First_needle_engraver) + deleter
  }
