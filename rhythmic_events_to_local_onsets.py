from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

class _Holder(object) : 
 def __init__(self, id) :
   self.id = id

class _Delete(DeleteStmt) :
  def __init__(self, time_next, local_onset) :
    def where_clause_fn(id) :
      strain = bound_range(id, time_next)
      stmt = select([strain]).where(strain.c.elt == local_onset.c.id)
      stmt = exists(stmt)
      return stmt
    DeleteStmt.__init__(self, local_onset, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, duration_log, duration, time_next, local_onset) :
    InsertStmt.__init__(self)
    '''
    problem...we only want to do this after all durations are set
    we can screen this by cutting things short if any duration_logs
    don't have durations.
    seems a bit kludgy...but oh well...
    '''
    devnull = easy_sj([duration_log,
                       Outerjoin(duration)],
                       use_id = True).cte(name='devnull')

    self.register_stmt(devnull)

    DEVNULL_CT =\
      select([func.count(devnull.c.id).label('total')]).\
       where(devnull.c.duration_num == None)

    duration_gcd_overshoot = den_overshoot(duration, "duration_gcd_overshoot")

    self.register_stmt(duration_gcd_overshoot)

    '''
    We pick the earliest element in a strain that does not have an onset
    as well as the previous onset or 0 if it does not exist.
    '''
    heads_of_lists = select([
      time_next.c.id.label('id')
    ]).where(DEVNULL_CT.c.total == 0).\
      except_(select([time_next.c.val]).\
        where(DEVNULL_CT.c.total == 0)).cte(name="heads_of_lists")

    self.register_stmt(heads_of_lists)

    until_first_onsetless_head = select([
      heads_of_lists.c.id.label('id'),
      heads_of_lists.c.id.label('prev_id'),
      local_onset.c.num.label('onset_num'),
      local_onset.c.den.label('onset_den'),
      duration.c.num.label('duration_num'),
      duration.c.den.label('duration_den'),
      literal(0).label('prev_onset_num'),
      literal(0).label('prev_onset_den'),
      literal(0).label('prev_duration_num'),
      literal(0).label('prev_duration_den'),
    ]).select_from(heads_of_lists.\
           join(duration, onclause = heads_of_lists.c.id == duration.c.id).\
           outerjoin(local_onset, onclause = heads_of_lists.c.id == local_onset.c.id)).\
        cte(name='until_first_onsetless_head', recursive=True)

    until_first_onsetless_head_a = until_first_onsetless_head.alias(name="until_first_onsetless_head_prev")
    until_first_onsetless_head = until_first_onsetless_head.union_all(
      select([
        time_next.c.val.label('id'),
        until_first_onsetless_head_a.c.id.label('prev_id'),
        local_onset.c.num.label('onset_num'),
        local_onset.c.den.label('onset_den'),
        duration.c.num.label('duration_num'),
        duration.c.den.label('duration_den'),
        until_first_onsetless_head_a.c.onset_num.label('prev_onset_num'),
        until_first_onsetless_head_a.c.onset_den.label('prev_onset_den'),
        until_first_onsetless_head_a.c.duration_num.label('prev_duration_num'),
        until_first_onsetless_head_a.c.duration_den.label('prev_duration_den')
      ]).select_from(time_next.\
           join(duration, onclause = time_next.c.val == duration.c.id).\
           outerjoin(local_onset, onclause = time_next.c.val == local_onset.c.id)).\
         where(time_next.c.id == until_first_onsetless_head_a.c.id).\
         where(until_first_onsetless_head_a.c.onset_num != None)
    )

    self.register_stmt(until_first_onsetless_head)

    durations_to_onsets = select([until_first_onsetless_head.c.id.label('id'),
        case([(until_first_onsetless_head.c.id != until_first_onsetless_head.c.prev_id,
               (until_first_onsetless_head.c.prev_duration_num *\
                   duration_gcd_overshoot.c.gcd /\
                   until_first_onsetless_head.c.prev_duration_den) +\
               (until_first_onsetless_head.c.prev_onset_num *\
                   duration_gcd_overshoot.c.gcd /\
                   until_first_onsetless_head.c.prev_onset_den))],
             else_ = 0).label('num'),
        duration_gcd_overshoot.c.gcd.label('den')]).\
      where(until_first_onsetless_head.c.onset_num == None).\
      cte("durations_to_onsets", recursive = True)

    durations_to_onsets_a =\
      durations_to_onsets.alias(name = "durations_to_onsets_prev")

    durations_to_onsets = durations_to_onsets.union_all(
      select([time_next.c.val.label('id'),
              (durations_to_onsets_a.c.num +\
                   (duration.c.num * duration_gcd_overshoot.c.gcd / duration.c.den)).label('num'),
              duration_gcd_overshoot.c.gcd.label('den')
            ]).\
      where(duration.c.id == durations_to_onsets_a.c.id).\
      where(time_next.c.id == durations_to_onsets_a.c.id))

    self.register_stmt(durations_to_onsets)

    durations_to_onsets_reduced = gcd_table(durations_to_onsets).cte(name="durations_to_onsets_reduced")

    self.register_stmt(durations_to_onsets_reduced)

    self.insert = local_onset.insert().from_select(['id','num','den'], select([durations_to_onsets_reduced]))

def generate_ddl(time_next, local_onset, duration_log, duration, LOG = False) :

  OUT = []

  insert_stmt = _Insert(duration_log, duration,
           time_next, local_onset)

  #del_stmt = _Delete(time_next, local_onset, start_with_id=False)
  ### uggghhh...should work across the board now
  del_stmt = _Delete(time_next, local_onset, start_with_id=True)

  del_stmt_tn_insert = _Delete(time_next, local_onset)

  OUT += [DDL_unit(duration, action, [del_stmt], [insert_stmt]) for action in ['INSERT', 'UPDATE', 'DELETE']]
  OUT.append(DDL_unit(time_next, 'DELETE', [del_stmt], [insert_stmt]))
  OUT.append(DDL_unit(time_next, 'INSERT', [del_stmt_tn_insert], [insert_stmt]))

  #OUT += create_universal_trigger(duration, del_stmt, insert_stmt, LOG)
  # this way, we avoid cases where strings are temporarily broken
  #OUT += create_triggers_on_events(time_next, del_stmt, insert_stmt, ['DELETE'], LOG)
  #OUT += create_triggers_on_events(time_next, del_stmt_tn_insert, insert_stmt, ['INSERT'], LOG)

  return OUT


if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  #MANUAL_DDL = True
  MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(time_next = Time_next,
                                     local_onset = Local_onset,
                                     duration_log = Duration_log,
                                     duration = Duration))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)


  stmts = []

  EXP = 8
  BIG = 2**EXP

  TIME_NEXT = {}
  DURATIONS = []
  for x in range(BIG) :
    # creates strains
    if x not in [a - b for a in [y * 2**(EXP - 2) for y in range(1,5)] for b in range(1,3)] :
      stmts.append((Time_next, {'id':x, 'val':x + 2}))
      #TIME_NEXT[x] = _Holder(x + 2)
    stmts.append((Duration_log, {'id':x, 'val':0}))
    stmts.append((Duration, {'id':x, 'num':((x % 3) + 1), 'den':4}))
    #DURATIONS.append((((x % 3) + 1), 4))

  '''
  def populate(key, tn, d, v=0) :
    if tn.has_key(key) :
      onset = v + d[key]
      tn[key].onset = onset
      populate(tn[key].id, tn, d, onset)

  for x in XX | YY :
    populate(x, TIME_NEXT, DURATIONS)
  '''
  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0], st[1], MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Local_onset])).fetchall() :
    print row
  
  manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  #conn.execute(Duration.update().values(num=100, den=1).where(Duration.c.id == 4))
  
  print "*************"
  print time.time() - NOW
  for row in conn.execute(select([Local_onset])).fetchall() :
    print row
