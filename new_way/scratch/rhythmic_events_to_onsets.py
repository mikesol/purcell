# nested recursive
# first level is onset_anchors
# second level is notes
# 
# get all items whose onset_anchors are None
#  time_next onset_anchor onset_offset

from sqlalchemy.sql.expression import literal
from plain import *

def make_statement(duration_log, duration, time_next, onset_anchor, onset_offset) :
  '''
  problem...we only want to do this after all durations are set
  we can screen this by cutting things short if any duration_logs don't have durations
  we build this into all requests
  seems a bit kludgy...but oh well...
  '''
  devnull = easy_sj([duration_log,
                     Outerjoin(duration),
                     Outerjoin(time_next)],
                     use_id = True)

  DEVNULL_CT =\
    select([func.count(devnull.c.id).label('total')]).\
     where(devnull.c.duration_val == None)

  # MAKE RECURSIVE DURATION LOG TO DURATIONS SUMMING DURATIONS
  # id      summed_duration id
  # next_id bigger          id
  # next_id bigger          id
  durations_to_summed_durations = select([
      duration.c.id.label('id'),
      literal(0.0).label('summed_duration'),
      duration.c.id.label('starting_id')]).\
    where(duration.c.id == onset_anchor.c.id).\
    where(DEVNULL_CT.c.total == 0).\
    cte("durations_to_summed_durations", recursive = True)

  durations_to_summed_durations_a =\
    durations_to_summed_durations.alias(name = "durations_to_summed_durations_prev")

  durations_to_summed_durations = durations_to_summed_durations.union_all(
    select([time_next.c.val,
            (durations_to_summed_durations_a.c.summed_duration +\
                 duration.c.val).label('summed_duration'),
            durations_to_summed_durations_a.c.starting_id]).\
    where(duration.c.id == durations_to_summed_durations_a.c.id).\
    where(time_next.c.id == durations_to_summed_durations_a.c.id))
  
  # for each onset, we need to build its time_next list and see
  # what other onsets are in it
  # can't have OPO before - need to build it here
  # we take all offsets that have none and find them in durations_to_summed_durations
  # this combines the two partial orderings
  onset_anchor_to_onset = select([
        onset_anchor.c.id.label('id'),
        (durations_to_summed_durations.c.summed_duration +\
          onset_offset.c.val).label('offset')
      ]).\
    select_from(onset_anchor.outerjoin(onset_offset,
          onclause = onset_anchor.c.id == onset_offset.c.id)).\
    where(onset_anchor.c.val == None).\
    where(durations_to_summed_durations.c.id == onset_anchor.c.id).\
    cte("onset_anchor_to_onset", recursive = True)
    
  onset_anchor_to_onset_a =\
        onset_anchor_to_onset.alias(name = "onset_anchor_to_onset_prev")

  onset_anchor_to_onset = onset_anchor_to_onset.union_all(
    select([onset_anchor.c.id,
            (onset_anchor_to_onset_a.c.offset +\
              onset_offset.c.val +\
              durations_to_summed_durations.c.summed_duration)
          ]).\
    select_from(onset_anchor.outerjoin(onset_offset,
          onclause = onset_anchor.c.id == onset_offset.c.id)).\
    where(onset_anchor.c.val ==\
        durations_to_summed_durations.c.id).\
    where(onset_anchor_to_onset_a.c.id ==\
        durations_to_summed_durations.c.starting_id))

  durations_to_onsets =\
     select([durations_to_summed_durations.c.id,
        (durations_to_summed_durations.c.summed_duration +\
          onset_anchor_to_onset.c.offset).label('offset')]).\
     select_from(durations_to_summed_durations.join(onset_anchor_to_onset,
         onclause = onset_anchor_to_onset.c.id ==\
             durations_to_summed_durations.c.starting_id))

  return select([durations_to_onsets])

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()

  stmt = make_statement(Duration_log, Duration, Time_next,
                        Onset_anchor, Onset_offset)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  EXP = 6
  BIG = 2**EXP

  for x in range(BIG) :
    # creates strains
    if x not in [a - b for a in [y * 2**(EXP - 2) for y in range(1,5)] for b in range(1,3)] :
      stmts.append(Time_next.insert().values(id=x, val=x + 2))
    # 
    if x in [a + b for a in [y * 2**(EXP-2) for y in range(4)] for b in range(2)] :
      stmts.append(Onset_anchor.insert().values(id=x, val= None if x < 2**(EXP-2) else (x - 2**(EXP-3))))
      stmts.append(Onset_offset.insert().values(id=x, val= 0.25 if x % 2 == 0 else 0.5))
    stmts.append(Duration_log.insert().values(id=x, val=0))
    stmts.append(Duration.insert().values(id=x, val=1.0))
  
  trans = conn.begin()
  for st in stmts :
    conn.execute(st)
  trans.commit()

  print stmt

  for row in conn.execute(stmt).fetchall() :
    print row

  #for row in conn.execute(select([Onset_anchor])).fetchall() :
  #  print row

  #for row in conn.execute(select([Time_next])).fetchall() :
  #  print row
