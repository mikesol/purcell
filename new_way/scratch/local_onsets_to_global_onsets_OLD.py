from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import sys

'''
def _make_delete_statement(time_next, local_onset, id_to_use = "@ID@", start_with_id=False) :
  # compose a list starting from this time next moving forwards
  # invalidate offsets of these ids
  strain = bound_range(id_to_use, time_next, start_with_id = start_with_id)
  stmt = select([strain]).where(strain.c.elt == local_onset.c.id)
  stmt = exists(stmt).compile(compile_kwargs={"literal_binds": True})
  return global_onset.delete().where(text(str(stmt)))
'''

def _make_backwards_forwards_iterator(time_next, local_onset, local_onsets_to_global_onsets, local_onset_gcd_overshoot, forwards) :
  name = "affected_forwards" if forwards else "affected_backwards"

  affected_stmt = select([
    time_next.c.id.label('id'),
    (local_onsets_to_global_onsets.c.num +\
           (local_onset.c.num * local_onset_gcd_overshoot.c.gcd / local_onset.c.den)).label('num'),
    local_onset_gcd_overshoot.c.gcd.label('den'),
    time_next.c.id.label('anchor'),
  ]).where(time_next.c.id == local_onsets_to_global_onsets.c.id).\
     where(local_onset.c.id == time_next.c.id).\
     cte(recursive=True, name=name)

  affected_stmt_a = affected_stmt.alias(name=name+"_prev")
  
  USING = time_next.c.id if not forwards else time_next.c.val
  FROM = time_next.c.val if not forwards else time_next.c.id

  affected_stmt = affected_stmt.union_all(
    select([
      USING,
      (local_onsets_to_global_onsets.c.num +\
           (local_onset.c.num * local_onset_gcd_overshoot.c.gcd / local_onset.c.den)).label('num'),
      local_onset_gcd_overshoot.c.gcd.label('den'),
      affected_stmt_a.c.anchor]).\
          where(FROM == affected_stmt_a.c.id).\
          where(local_onsets_to_global_onsets.c.id == affected_stmt_a.c.anchor).\
          where(local_onset.c.id == USING)
  )

  return affected_stmt


def _make_insert_statement(global_onset, time_next, local_onset, onset_anchor, duration_log, short=False) :
  '''
  all things with duration logs must have local onsets
[B  '''
  devnull = easy_sj([duration_log,
                     Outerjoin(local_onset)],
                     use_id = True)

  DEVNULL_CT =\
    select([func.count(devnull.c.id).label('total')]).\
     where(devnull.c.local_onset_num == None)

  local_onset_gcd_distinct_dens =\
    select([distinct(local_onset.c.den).label('den')]).cte(name="distinct_dens")

  local_onset_gcd_overshoot =\
    select([product_i(local_onset_gcd_distinct_dens.c.den).label('gcd')]).cte(name="distinct_dens_overshoot")

  '''
  We pick the earliest element in a strain that does not have an onset
  as well as the previous onset or 0 if it does not exist.
  '''
  heads_of_lists = select([
    onset_anchor.c.id.label('id')
  ]).where(DEVNULL_CT.c.total == 0).\
    except_(select([onset_anchor.c.val]).\
      where(DEVNULL_CT.c.total == 0)).cte(name="heads_of_lists")

  until_first_global_onsetless_head = select([
    heads_of_lists.c.id.label('id'),
    heads_of_lists.c.id.label('prev_id'),
    global_onset.c.num.label('global_onset_num'),
    global_onset.c.den.label('global_onset_den'),
    local_onset.c.num.label('local_onset_num'),
    local_onset.c.den.label('local_onset_den'),
    literal(0).label('prev_global_onset_num'),
    literal(0).label('prev_global_onset_den'),
    literal(0).label('prev_local_onset_num'),
    literal(0).label('prev_local_onset_den'),
  ]).select_from(heads_of_lists.\
         join(local_onset, onclause = heads_of_lists.c.id == local_onset.c.id).\
         outerjoin(global_onset, onclause = heads_of_lists.c.id == global_onset.c.id)).\
      cte(name='until_first_global_onsetless_head', recursive=True)

  until_first_global_onsetless_head_a = until_first_global_onsetless_head.alias(name="until_first_global_onsetless_head_prev")
  until_first_global_onsetless_head = until_first_global_onsetless_head.union_all(
    select([
      onset_anchor.c.val.label('id'),
      until_first_global_onsetless_head_a.c.id.label('prev_id'),
      global_onset.c.num.label('global_onset_num'),
      global_onset.c.den.label('global_onset_den'),
      local_onset.c.num.label('local_onset_num'),
      local_onset.c.den.label('local_onset_den'),
      until_first_global_onsetless_head_a.c.global_onset_num.label('prev_global_onset_num'),
      until_first_global_onsetless_head_a.c.global_onset_den.label('prev_global_onset_den'),
      until_first_global_onsetless_head_a.c.local_onset_num.label('prev_local_onset_num'),
      until_first_global_onsetless_head_a.c.local_onset_den.label('prev_local_onset_den')
    ]).select_from(onset_anchor.\
         join(local_onset, onclause = onset_anchor.c.val == local_onset.c.id).\
         outerjoin(global_onset, onclause = onset_anchor.c.val == global_onset.c.id)).\
       where(onset_anchor.c.id == until_first_global_onsetless_head_a.c.id).\
       where(until_first_global_onsetless_head_a.c.global_onset_num != None)
  )

  local_onsets_to_global_onsets = select([until_first_global_onsetless_head.c.id.label('id'),
      case([(until_first_global_onsetless_head.c.id != until_first_global_onsetless_head.c.prev_id,
             (until_first_global_onsetless_head.c.prev_local_onset_num *\
                 local_onset_gcd_overshoot.c.gcd /\
                 until_first_global_onsetless_head.c.prev_local_onset_den) +\
             (until_first_global_onsetless_head.c.prev_global_onset_num *\
                 local_onset_gcd_overshoot.c.gcd /\
                 until_first_global_onsetless_head.c.prev_global_onset_den))],
           else_ = 0).label('num'),
      local_onset_gcd_overshoot.c.gcd.label('den')]).\
    where(until_first_global_onsetless_head.c.global_onset_num == None).\
    cte("local_onsets_to_global_onsets", recursive = True)

  local_onsets_to_global_onsets_a =\
    local_onsets_to_global_onsets.alias(name = "local_onsets_to_global_onsets_prev")

  local_onsets_to_global_onsets = local_onsets_to_global_onsets.union_all(
    select([onset_anchor.c.val.label('id'),
            (local_onsets_to_global_onsets_a.c.num +\
                 (local_onset.c.num * local_onset_gcd_overshoot.c.gcd / local_onset.c.den)).label('num'),
            local_onset_gcd_overshoot.c.gcd.label('den')
          ]).\
    where(local_onset.c.id == local_onsets_to_global_onsets_a.c.id).\
    where(onset_anchor.c.id == local_onsets_to_global_onsets_a.c.id))


  affected_backwards = _make_backwards_forwards_iterator(time_next, local_onset, local_onsets_to_global_onsets, local_onset_gcd_overshoot, False)
  affected_forwards = _make_backwards_forwards_iterator(time_next, local_onset, local_onsets_to_global_onsets, local_onset_gcd_overshoot, True)

  # union to prevent duplicates
  everything = select([affected_backwards.c.id.label('id'),
                       affected_backwards.c.num.label('num'),
                       affected_backwards.c.den.label('den')]).union(
                 select([affected_forwards.c.id.label('id'),
                         affected_forwards.c.num.label('num'),
                         affected_forwards.c.den.label('den')]))


  out = global_onset.insert().from_select(['id','num','den'], select([everything]))
  if (short) :
    #return heads_of_lists.element
    #return until_first_global_onsetless_head.element
    return heads_of_lists.element
  return out

'''
def generate_ddl(time_next, local_onset, duration_log, duration, LOG = False) :

  OUT = []

  insert_stmt = _make_insert_statement(duration_log, duration,
           time_next, local_onset).\
                compile(compile_kwargs={"literal_binds": True})

  del_stmt = _make_delete_statement(time_next, local_onset).\
         compile(compile_kwargs={"literal_binds": True})

  del_stmt_tn_insert = _make_delete_statement(time_next, local_onset, start_with_id=True).\
         compile(compile_kwargs={"literal_binds": True})

  OUT += create_universal_trigger(duration, del_stmt, insert_stmt, LOG)
  # this way, we avoid cases where strings are temporarily broken
  OUT += create_triggers_on_events(time_next, del_stmt, insert_stmt, ['DELETE'], LOG)
  OUT += create_triggers_on_events(time_next, del_stmt_tn_insert, insert_stmt, ['INSERT'], LOG)

  return OUT
'''

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  #ddls = generate_ddl(time_next = Time_next,
  #                    local_onset = Local_onset,
  #                    duration_log = Duration_log,
  #                    duration = Duration)

  #for ddl in ddls :
  #  event.listen(ddl.table, 'after_create', DDL(ddl.instruction).\
  #      execute_if(dialect='sqlite'))

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  NS = 2
  NV = 4
  NN = 64

  CT = 0
  for x in range(NS) :
    for y in range(NV) :
      for z in range(NN) :
        stmts.append(Duration_log.insert().values(id = CT, val = 0))
        stmts.append(Duration.insert().values(id = CT, num = 1, den = 1))
        stmts.append(Local_onset.insert().values(id = CT, num = z, den = 1))
        if (z == 0) and (y > 0) :
          # middle of previous voice
          stmts.append(Onset_anchor.insert().values(id = CT, val = (x * NS) + ((y - 1) * NV) + (NN / 2)))
        if z != NN - 1 :
          stmts.append(Time_next.insert().values(id = CT, val = CT + 1 ))
        CT += 1

  stmt = _make_insert_statement(Global_onset, Time_next, Local_onset, Onset_anchor, Duration_log)

  trans = conn.begin()
  for st in stmts :
    conn.execute(st)
  trans.commit()

  NOW = time.time()
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row

  stmt2 = _make_insert_statement(Global_onset, Time_next, Local_onset, Onset_anchor, Duration_log, short=True)

  for row in conn.execute(stmt2).fetchall() :
    print row

  conn.execute(stmt)
  #conn.execute(Duration.update().values(num=100, den=1).where(Duration.c.id == 4))
  
  print "*************"
  print time.time() - NOW
  #for row in conn.execute(select([Global_onset])).fetchall() :
  #  print row
