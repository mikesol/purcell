from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import sys

class _Delete(DeleteStmt) :
  def __init__(self, next_table, graphical_next) :
    def where_clause_fn(id) :
      strain = bound_range(id, next_table)
      stmt = select([strain]).where(strain.c.elt == graphical_next.c.id)
      stmt = exists(stmt)
      return stmt
    DeleteStmt.__init__(self, graphical_next, where_clause_fn)

# UGGGGGGGHHH,
# need this to = val, not id
# need to find a way to pass that in
# for now, just delete everything
# ugh ugh ugh ugh ugh ugh ugh ugh ugh ugh
# P.S.
# ugh
class _Delete_HTA(DeleteStmt) :
  def __init__(self, graphical_next) :
    def where_clause_fn(id) :
      return graphical_next.c.id != 3.1416
    DeleteStmt.__init__(self, graphical_next, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, horstemps_anchor, horstemps_next, time_next, graphical_next) :
    InsertStmt.__init__(self)

    horstemps_next_heads = select([
      horstemps_next.c.id.label('id')
    ]).except_(select([horstemps_next.c.val])).\
         cte(name="horstemps_next_heads")

    self.register_stmt(horstemps_next_heads)

    out_of_horstemps = select([
      horstemps_next.c.id.label('id'),
      horstemps_anchor.c.val.label('val')
    ]).select_from(
       horstemps_next.join(horstemps_anchor, onclause = horstemps_next.c.id == horstemps_anchor.c.id)
      ).\
        where(horstemps_next.c.val == None).cte(name="out_of_horstemps")

    self.register_stmt(out_of_horstemps)

    into_horstemps = select([
      time_next.c.id.label('id'),
      horstemps_next_heads.c.id.label('val')
    ]).select_from(time_next.join(horstemps_anchor, onclause = time_next.c.val == horstemps_anchor.c.val).\
          join(horstemps_next_heads, onclause = horstemps_anchor.c.id == horstemps_next_heads.c.id)).\
       cte(name="into_horstemps")
    
    self.register_stmt(into_horstemps)
    
    pure_time_next = select([
      time_next.c.id.label('id'),
      time_next.c.val.label('val')
    ]).select_from(time_next.outerjoin(horstemps_anchor, onclause = time_next.c.val == horstemps_anchor.c.val)).\
        where(horstemps_anchor.c.id == None).cte(name='pure_time_next')

    self.register_stmt(pure_time_next)

    merge_0 = select([pure_time_next.c.id.label('id'), pure_time_next.c.val.label('val')]).\
       union_all(select([into_horstemps.c.id, into_horstemps.c.val])).cte(name='merge_0')

    self.register_stmt(merge_0)

    merge_1 = select([merge_0.c.id.label('id'), merge_0.c.val.label('val')]).\
       union_all(select([out_of_horstemps.c.id, out_of_horstemps.c.val])).cte(name='merge_1')

    self.register_stmt(merge_1)

    # need to make sure it is not None to avoid duplicates
    merge_2 = select([merge_1.c.id.label('id'), merge_1.c.val.label('val')]).\
       union_all(select([horstemps_next.c.id, horstemps_next.c.val]).where(horstemps_next.c.val != None)).cte(name='merge_2')

    self.register_stmt(merge_2)
    
    giant_kludge = realize(merge_2, graphical_next, 'val')

    self.register_stmt(giant_kludge)

    self.insert = simple_insert(graphical_next, giant_kludge)

def generate_ddl(horstemps_anchor, horstemps_next, time_next, graphical_next) :

  OUT = []

  insert_stmt = _Insert(horstemps_anchor, horstemps_next, time_next, graphical_next)

  del_stmt_horstemps_next = _Delete(horstemps_next, graphical_next)
  del_stmt_horstemps_anchor = _Delete_HTA(graphical_next)
  del_stmt_time_next = _Delete(time_next, graphical_next)

  OUT += [DDL_unit(horstemps_next, action, [del_stmt_horstemps_next], [insert_stmt]) for action in ['INSERT', 'UPDATE', 'DELETE']]
  OUT += [DDL_unit(horstemps_anchor, action, [del_stmt_horstemps_anchor], [insert_stmt]) for action in ['INSERT', 'UPDATE', 'DELETE']]
  OUT += [DDL_unit(time_next, action, [del_stmt_time_next], [insert_stmt]) for action in ['INSERT', 'UPDATE', 'DELETE']]

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

  manager = DDL_manager(generate_ddl(horstemps_anchor = Horstemps_anchor,
                                     horstemps_next = Horstemps_next,
                                     time_next = Time_next,
                                     graphical_next = Graphical_next))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)


  stmts = []

  EXP = 8
  BIG = 2**EXP

  for x in range(BIG) :
    stmts.append((Time_next, {'id':x, 'val':x + 2}))
    if x > BIG - 3 :
      stmts.append((Time_next, {'id':x + 2, 'val':None}))

  CT = 0
  ANCHORS = [4,15,12,7]
  OFFSET = BIG + 2
  for x in range(16) :
    stmts.append((Horstemps_next, {'id': OFFSET + x, 'val': OFFSET + x + 4}))
    stmts.append((Horstemps_anchor, {'id': OFFSET + x, 'val': ANCHORS[CT % 4]}))
    if x > (16 - 5) :
      stmts.append((Horstemps_next, {'id': OFFSET + x + 4, 'val':None}))
      stmts.append((Horstemps_anchor, {'id': OFFSET + x + 4, 'val':ANCHORS[CT % 4]}))
    CT += 1

  #for stmt in stmts :
  #  print stmt[0].name, stmt[1]

  #sys.exit(1)

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  goods = conn.execute(select([Graphical_next])).fetchall()
  for START in [0,1] :
    V = START
    while True :
      l = filter(lambda x : x[0] == V, goods)
      if len(l) > 1 : raise ValueError
      if len(l) == 0 : break
      print l[0]
      V = l[0][1]
    print "*****"
  #for row in conn.execute(select([Graphical_next])).fetchall() :
  #  print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  #conn.execute(Duration.update().values(num=100, den=1).where(Duration.c.id == 4))
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
