from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

class _Holder(object) : 
 def __init__(self, id) :
   self.id = id

class _Delete(DeleteStmt) :
  def __init__(self, time_next, local_onset, start_with_id) :
    def where_clause_fn(id) :
      strain = bound_range(id, time_next, start_with_id = start_with_id)
      stmt = select([strain]).where(strain.c.elt == local_onset.c.id)
      stmt = exists(stmt)
      return stmt
    DeleteStmt.__init__(self, local_onset, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, horstemps_referent, horstemps_anchor, onset_referent, global_onset) :
    InsertStmt.__init__(self)

    # need to get global onsets to nonmusicals too...can be done in single
    # statement, so for now not property
    horstemps_to_global_onset = select([horstemps_referent.c.id, global_onset.c.num, global_onset.c.den]).\
       select_from(horstemps_referent.join(horstemps_anchor, onclause = horstemps_referent.c.val == horstemps_anchor.c.id).\
         join(global_onset, onclause = horstemps_anchor.c.val == global_onset.c.id)).cte(name="horstemps_to_global_onset")
    
    self.register_stmt(horstemps_to_global_onset)
    # we work off of all things that do not have a column
    # we order all musical things as * 2
    self.insert = ###

def generate_ddl(time_next, local_onset, duration_log, duration, LOG = False) :
  '''
  OUT = []

  insert_stmt = _Insert(duration_log, duration,
           time_next, local_onset)

  #del_stmt = _Delete(time_next, local_onset, start_with_id=False)
  ### uggghhh...should work across the board now
  del_stmt = _Delete(time_next, local_onset, start_with_id=True)

  del_stmt_tn_insert = _Delete(time_next, local_onset, start_with_id=True)

  OUT += [DDL_unit(duration, action, [del_stmt], [insert_stmt]) for action in ['INSERT', 'UPDATE', 'DELETE']]
  OUT.append(DDL_unit(time_next, 'DELETE', [del_stmt], [insert_stmt]))
  OUT.append(DDL_unit(time_next, 'INSERT', [del_stmt_tn_insert], [insert_stmt]))

  #OUT += create_universal_trigger(duration, del_stmt, insert_stmt, LOG)
  # this way, we avoid cases where strings are temporarily broken
  #OUT += create_triggers_on_events(time_next, del_stmt, insert_stmt, ['DELETE'], LOG)
  #OUT += create_triggers_on_events(time_next, del_stmt_tn_insert, insert_stmt, ['INSERT'], LOG)

  return OUT
  '''

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
