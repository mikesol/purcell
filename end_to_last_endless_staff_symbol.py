from sqlalchemy import Column, Integer, Float, and_, or_, case

def E(_13) :

  ####################################
  # assign end to all onsets from durations

  # get all score, staff, staff_symbol, onset, OUTTER JOIN end | callthis A
  # make alias B, group by score, staff, max onset 
  # select score, staff, onset FROM inner join B to A at score, staff, onset equal WHERE end is null | call this C
  # ...
  # get all score, staff, NOTE OR REST OR SILENCE, OUTTER JOIN end WHERE end IS NOT null (meaning IJOIN)
  # group by score, staff, max end | call this D
  # ...
  # select ID, END if end is not null else ONSET FROM outter join C and D on score and staff

  '''
  # 13 max should return a statement with correct labels (onset_num_max, onset_den_max)
  # this should automatically join the the dynamically created table
  Last_staff_symbols =\
    _13.K(_13.join(_13.Score.val, _13.Staff.val, _13.max(Onset), fake=[13.Name]).\
          filter(_13.Name.val == "staff_symbol").\
          group_by(_13.Score.val, _13.Staff.val).\
          with_labels().\
          subquery(name="last_staff_symbols"))

  All_staff_symbols =\
    _13.K(_13.last_outerjoin(_13.Score.id, _13.Score.val,
                          _13.Staff.val, _13.Name.val,
                          _13.Onset.val, _13.End.val).\
      filter(_13.Name.val == "staff_symbol").\
      group_by(_13.Score.val, _13.Staff.val).\
      with_labels().\
      subquery(name="last_staff_symbols")

  Endless_last_staff_symbols =\
    _13.K(
      _13.session.query(
        All_staff_symbols.score_id,
        All_staff_symbols.score_val,
        All_staff_symbols.staff_val,
        All_staff_symbols.onset_val).\
      select_from(All_staff_symbols).\
      join(Last_staff_symbols, and_(
        All_staff_symbols.score_val == Last_staff_symbols.score_val,
        All_staff_symbols.staff_val == Last_staff_symbols.staff_val,
        All_staff_symbols.onset_max == Last_staff_symbols.onset)).\
      filter(All_staff_symbols.end_val == None).\
      subquery(name="endless_staff_symbols"))

  All_staff_symbol_end_anchors =
    _13.K(_13.join(_13.Score.val,
                   _13.Staff.val,
                   _13.max(End),
                   fake=[_13.Name]).\
      filter(or_(_13.Name.val == "note",
        _13.Name.val == "rest",
        _13.Name.val == "space")).\
      group_by(_13.Score.val, _13.Staff.val).\
      with_labels().\
      subquery(name="all_staff_symbol_end_anchors")

  new_ends =_13.query(Endless_last_staff_symbols.score_id, 
                      _13.case([(All_staff_symbol_ends.end_val == None,
                       Endless_last_staff_symbols.onset_val)],
                     else_ = All_staff_symbol_ends.end_val)).\
      select_from(Endless_last_staff_symbols).\
      outerjoin(All_staff_symbol_end_anchors,
        and_(All_staff_symbol_end_anchors.score_val ==\
               Endless_last_staff_symbols.score_val,
             All_staff_symbol_ends_anchors.staff_val ==\
               Endless_last_staff_symbols.staff_val
        ))
  '''

  last_staff_symbols =\
    _13.join(
        _13.session.query(_13.Score.val, _13.Staff.val,
                          _13.Onset.max),
        _13.Score,
        [_13.Staff, _13.Name, _13.Onset]).\
      filter(_13.Name.val == "staff_symbol").\
      group_by(_13.Score.val, _13.Staff.val).\
      with_labels().\
      subquery(name="last_staff_symbols")

  all_staff_symbols =\
    _13.last_outerjoin(
        _13.session.query(_13.Score.id, _13.Score.val,
                          _13.Staff.val, _13.Name.val,
                          _13.Onset.val, _13.Onset.float,
                          _13.End.val),
        _13.Score,
        [_13.Staff, _13.Name, _13.Onset, _13.End]).\
      filter(_13.Name.val == "staff_symbol").\
      with_labels().\
      subquery(name="all_staff_symbols")

  instr = [Column('score__id', Integer), _13.Score, _13.Staff, _13.Name,
           _13.Onset, Column('onset__float', Float), _13.End]
  
  All_staff_symbols = _13.map(all_staff_symbols,
                              "All_staff_symbols",
                              *instr)

  Last_staff_symbols = _13.map(last_staff_symbols,
                              "Last_staff_symbols",
                              _13.Score, _13.Staff,
                              Column('onset__max', Float))

  endless_last_staff_symbols =\
    _13.session.query(
      All_staff_symbols.score__id.label('score__id'),
      All_staff_symbols.score__val.label('score__val'),
      All_staff_symbols.staff__val.label('staff__val'),
      All_staff_symbols.onset__val.label('onset__val')).\
    select_from(All_staff_symbols).\
    join(Last_staff_symbols, and_(
      All_staff_symbols.score__val == Last_staff_symbols.score__val,
      All_staff_symbols.staff__val == Last_staff_symbols.staff__val,
      All_staff_symbols.onset__float == Last_staff_symbols.onset__max)).\
    filter(All_staff_symbols.end__val == None).\
    subquery(name="endless_staff_symbols")

  Endless_last_staff_symbols = _13.map(endless_last_staff_symbols,
                                  "Endless_last_staff_symbols",
                                  Column('score__id', Integer),
                                  _13.Score,
                                  _13.Staff,
                                  _13.Onset)

  # ugh... will need to join to get values out
  all_staff_symbol_end_anchors =\
    _13.join(
        _13.session.query(_13.Score.val,
                          _13.Staff.val,
                          _13.End.max.label('end__max')),
        _13.Score,
        [_13.Staff, _13.Name, _13.End]).\
      filter(or_(_13.Name.val == "note",
        _13.Name.val == "rest",
        _13.Name.val == "space")).\
      group_by(_13.Score.val, _13.Staff.val).\
      with_labels().\
      subquery(name="all_staff_symbol_end_anchors")

  all_staff_symbol_end_anchors_kludge =\
    _13.join(
        _13.session.query(_13.Score.val,
                          _13.Staff.val,
                          _13.End.val,
                          _13.End.float.label('end__float')),
        _13.Score,
        [_13.Staff, _13.Name, _13.End]).\
      filter(or_(_13.Name.val == "note",
        _13.Name.val == "rest",
        _13.Name.val == "space")).\
      with_labels().\
      subquery(name="all_staff_symbol_end_anchors_kludge")

  All_staff_symbol_end_anchors = _13.map(all_staff_symbol_end_anchors,
                                     "All_staff_symbol_end_anchors",
                                    _13.Score,
                                    _13.Staff,
                                     Column('end__max', Float))

  All_staff_symbol_end_anchors_kludge = _13.map(all_staff_symbol_end_anchors_kludge,
                                     "All_staff_symbol_end_anchors_kludge",
                                    _13.Score,
                                    _13.Staff,
                                    _13.End,
                                     Column('end__float', Float))

  all_staff_symbol_ends =\
        _13.session.query(All_staff_symbol_end_anchors_kludge.score__val.label("score__val"),
                          All_staff_symbol_end_anchors_kludge.staff__val.label("staff__val"),
                          All_staff_symbol_end_anchors_kludge.end__val.label("end__val")).\
          select_from(All_staff_symbol_end_anchors).\
          join(All_staff_symbol_end_anchors_kludge, and_(
            All_staff_symbol_end_anchors_kludge.score__val == All_staff_symbol_end_anchors.score__val,
            All_staff_symbol_end_anchors_kludge.staff__val == All_staff_symbol_end_anchors.staff__val,
            All_staff_symbol_end_anchors_kludge.end__float == All_staff_symbol_end_anchors.end__max)).\
          subquery("all_staff_symbol_ends")

  All_staff_symbol_ends = _13.map(all_staff_symbol_ends,
                                     "All_staff_symbol_end_anchors_kludge",
                                    _13.Score,
                                    _13.Staff,
                                    _13.End,
                                     Column('end__float', Float))

   #### UGGGGHHHH
  new_ends =_13.session.query(Endless_last_staff_symbols.score__id, 
                case([(All_staff_symbol_ends.end__val == None,
                       Endless_last_staff_symbols.onset__num)],
                     else_ = All_staff_symbol_ends.end__num),
                case([(All_staff_symbol_ends.end__val == None,
                       Endless_last_staff_symbols.onset__den)],
                     else_ = All_staff_symbol_ends.end__den)).\
      select_from(Endless_last_staff_symbols).\
      outerjoin(All_staff_symbol_ends,
        and_(All_staff_symbol_ends.score__val == Endless_last_staff_symbols.score__val,
             All_staff_symbol_ends.staff__val == Endless_last_staff_symbols.staff__val
        ))

  return {
    "inserter" : [_13.insert(_13.End, new_ends)],
  }
  '''
    "debug" : [last_staff_symbols.element,
               all_staff_symbols.element,
               endless_last_staff_symbols.element,
               all_staff_symbol_end_anchors.element,
               all_staff_symbol_end_anchors_kludge.element,
               all_staff_symbol_ends.element,
               new_ends.statement]
  '''