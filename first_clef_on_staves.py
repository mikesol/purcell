from sqlalchemy import Column, Integer, Float, and_, delete

def E(_13) :

  ####################################
  # assign first clef to all staves
  first_events_on_staffs =\
    _13.join(_13.session.query(_13.Score._a[0].val,
                           _13.Staff._a[0].val,
                           _13.Onset._a[0].val,
                           _13.Onset._a[0].min.label('onset_min')),
             _13.Score._a[0], [_13.Staff._a[0],
                               _13.Onset._a[0]]).\
      group_by(_13.Score._a[0].val, _13.Staff._a[0].val).\
      order_by('onset_min').\
      with_labels().\
      subquery(name="first_events_on_staffs")

  first_clefs_on_staffs =\
    _13.join(_13.session.query(_13.Score._a[1].val,
                           _13.Staff._a[1].val,
                           _13.Onset._a[1].val,
                           _13.Onset._a[1].min.label('onset_min'),
                           _13.Name._a[1]),
             _13.Score._a[1], [_13.Staff._a[1],
                               _13.Onset._a[1],
                               _13.Name._a[1]]).\
      filter(_13.Name._a[1].val == "clef").\
      group_by(_13.Score._a[1].val, _13.Staff._a[1].val).\
      order_by('onset_min').\
      with_labels().\
      subquery(name="first_celfs_on_staffs")

  clef_instr = [_13.Score, _13.Staff, _13.Onset, Column('onset_min', Float)]

  First_events_on_staffs = _13.map(first_events_on_staffs,
                                   "First_events_on_staffs",
                                   *clef_instr)
  First_clefs_on_staffs = _13.map(first_clefs_on_staffs,
                                  "First_clefs_on_staffs",
                                  *(clef_instr+[_13.Name]))

  joined_firsts_to_clefs = _13.session.query(
                             First_events_on_staffs.score__val,
                             First_events_on_staffs.staff__val,
                             First_events_on_staffs.onset__val,
                             "'clef'").outerjoin(
    First_clefs_on_staffs,
      and_(First_events_on_staffs.score__val ==\
             First_clefs_on_staffs.score__val,
           First_events_on_staffs.staff__val ==\
             First_clefs_on_staffs.staff__val,
           First_events_on_staffs.onset__val ==\
             First_clefs_on_staffs.onset__val)).\
    filter(First_clefs_on_staffs.name__val == None).\
    subquery(name="joined_firsts_to_clefs")

  First_clef_engraver = _13.map(_13.metadata, "First_clef_engraver",
     Column('id', Integer, primary_key=True),
     _13.Score, _13.Staff, _13.Onset, _13.Name, Column('pointer', Integer))

  clef_insert = _13.t(First_clef_engraver).insert().from_select(
       _13.names(_13.Score, _13.Staff, _13.Onset, _13.Name),
       joined_firsts_to_clefs)

  deleter = [delete(_13.t(First_clef_engraver))]

  return {
    "tables" : [_13.t(First_clef_engraver)],
    "inserter" : [clef_insert],
    "pointer" : _13.make_pointer_assigner(First_clef_engraver),
    "updater" : _13.make_updater(First_clef_engraver) + deleter
  }
