from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# uggghhh...one note voice

class _Delete(DeleteStmt) :
  def __init__(self, time_next, local_onset, start_with_id) :
    def where_clause_fn(id) :
      strain = bound_range(id, time_next, start_with_id = start_with_id)
      stmt = select([strain]).where(strain.c.elt == local_onset.c.id)
      stmt = exists(stmt)
      return stmt
    DeleteStmt.__init__(self, local_onset, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, duration_log, duration, time_next, local_onset) :
    InsertStmt.__init__(self)
    
    time_next_heads = select([
      time_next.c.id.label('id')
    ]).except_(select([time_next.c.val]).\
         cte(name="time_next_heads")

    self.register_stmt(time_next_heads)

    horstemps_next_heads = select([
      horstemps_next.c.id.label('id')
    ]).except_(select([horstemps_next.c.val]).\
         cte(name="horstemps_next_heads")

    self.register_stmt(horstemps_next_heads)

    horstemps_next_heads_w_ref = select([
      horstemps_next_heads.c.id.label('id'),
      horstemps_referent.c.val.label('ref'),
    ]).where(horstemps_next_heads.c.id == horstemps_referent.c.id).\
        cte(name="horstemps_next_heads_w_ref")

    self.register_stmt(horstemps_next_heads_w_ref)

    real_horstemps_next_heads = select([
      horstemps_next.c.id.label('id')
    ]).select_from(horstemps_next.join(horstemps_referent, onclause = horstemps_next.c.id == horstemps_referent_c.id).join(horstemps_anchor, onclause = horstemps_referent.c.val == horstemps_anchor.c.id)).\
         where(horstemps_anchor.c.val != time_next_heads.c.id).\
       except_(select([horstemps_next.c.val]).\
         cte(name="real_horstemps_next_heads")

    self.register_stmt(real_horstemps_next_heads)

    real_time_next_heads = select([
      time_next_heads.c.id.label('id')
    ]).where(time_next_heads.c.id != horstemps_anchor.c.val).\
      cte(name="real_time_next_heads")

    self.register_stmt(real_time_next_heads)

    starting_ids = select([
      real_time_next_heads.c.id.label('id'),
      case([(horstemps_anchor.c.id != None, horstemps_next_heads_w_ref.c.id)], else_ = time_next.c.val).label('val')
    ]).select_from(real_time_next_heads.\
            join(time_next, onclause = time_next.c.id == real_time_next_heads.c.id).\
            outerjoin(horstemps_anchor, onclause = horstemps_anchor.c.val == time_next.c.val).\
            join(horstemps_next_heads_w_ref, onclause = horstemps_next_heads_w_ref.c.ref == horstemps_anchor.c.id)).\
        union_all(select([real_horstemps_next_heads.c.id,
             case([(horstemps_next.val == None, horstemps_anchor.c.val)], else_ = horstemps_next.val)]).select_from(
               real_horstemps_next_heads.join(horstemps_referent, onclause = real_horstemps_next_heads.c.id == horstemps_referent.c.id).\
               join(horstemps_anchor, onclause = horstemps_referent.c.val == horstemps_anchor.c.id)
             )
             ).\
          cte(name="graphical_next", recursive = True)

    prev_ids = starting_ids.alias(name="graphical_next_prev")

    graphical_next = starting_ids.union_all(select([
      real_time_next_heads.c.id.label('id'),
      case([(horstemps_anchor.c.id != None, horstemps_next_heads_w_ref.c.id)], else_ = time_next.c.val).label('val')
    ]).select_from(real_time_next_heads.\
            join(time_next, onclause = time_next.c.id == real_time_next_heads.c.id).\
            outerjoin(horstemps_anchor, onclause = horstemps_anchor.c.val == time_next.c.val).\
            join(horstemps_next_heads_w_ref, onclause = horstemps_next_heads_w_ref.c.ref == horstemps_anchor.c.id)).\
        union_all(select([real_horstemps_next_heads.c.id,
             case([(horstemps_next.val == None, horstemps_anchor.c.val)], else_ = horstemps_next.val)]).select_from(
               real_horstemps_next_heads.join(horstemps_referent, onclause = real_horstemps_next_heads.c.id == horstemps_referent.c.id).\
               join(horstemps_anchor, onclause = horstemps_referent.c.val == horstemps_anchor.c.id)
             )
             ))

    starting_ids_a = starting_ids.alias(name="starting_ids_prev")

    self.insert = local_onset.insert().from_select(['id','num','den'], select([durations_to_onsets_reduced]))

def generate_ddl(time_next, local_onset, duration_log, duration, LOG = False) :

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
