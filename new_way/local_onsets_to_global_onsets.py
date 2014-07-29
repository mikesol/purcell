from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import sys

'''
when a local onset is deleted, its global onset is invalid
if the object is an anchor, all objects that refer to it
  have a global onset that is now invalid

when an onset referent is deleted, its global onset is invalid
if the object is an anchor, all objects that refer to it
  have a global onset that is now invalid

when an onset anchor is deleted, its global onset is invalid
if the object is an anchor (which it is by def...), all objects that refer to it
  have a global onset that is now invalid
'''

class _Delete(DeleteStmt) :
  def __init__(self, global_onset, onset_referent, onset_anchor, local_onset) :
    def where_clause_fn(id) :
      # for now, recalculate everything
      # make more fine tuned later
      return global_onset.c.id == global_onset.c.id

    DeleteStmt.__init__(self, global_onset, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, global_onset, onset_referent, onset_anchor, local_onset) :
    InsertStmt.__init__(self)
    # ditto...disgustingly time consuming but for now possible

    '''
    first, find things without an onset referent
    for these, their local offset must be their global offset

    THEN, find all things that have an onset_referent whose onset_anchor value is the id in question
    add, go on
    '''

    local_onset_den = den_overshoot(local_onset, "local_onset_den")

    onset_anchor_tree_head = select([
          local_onset.c.id.label('id'),
          (local_onset.c.num * local_onset_den.c.gcd / local_onset.c.den).label('num'),
          local_onset_den.c.gcd.label('den'),
      ]).select_from(local_onset.outerjoin(onset_referent, onclause = local_onset.c.id == onset_referent.c.id)).\
          where(onset_referent.c.val == None).\
      cte(name="onset_anchor_tree_head", recursive = True)

    self.register_stmt(onset_anchor_tree_head)

    onset_anchor_tree_head_a = onset_anchor_tree_head.alias(name = "onset_anchor_tree_head_prev")

    onset_anchor_tree = onset_anchor_tree_head.union_all(
      select([
        local_onset.c.id.label('id'),
        onset_anchor_tree_head_a.c.num + (local_onset.c.num * local_onset_den.c.gcd / local_onset.c.den),
        local_onset_den.c.gcd,
      ]).select_from(onset_referent.join(onset_anchor, onclause = onset_referent.c.val == onset_anchor.c.id)).\
         where(onset_anchor.c.val == onset_anchor_tree_head_a.c.id).\
         where(local_onset.c.id == onset_referent.c.id)
    )

    self.register_stmt(onset_anchor_tree)
    
    onset_anchor_tree_vierge = select([
      onset_anchor_tree.c.id.label('id'),
      onset_anchor_tree.c.num.label('num'),
      onset_anchor_tree.c.den.label('den')
    ]).select_from(onset_anchor_tree.\
      outerjoin(global_onset, onclause = onset_anchor_tree.c.id == global_onset.c.id)).\
      where(global_onset.c.num == None).cte(name = "onset_anchor_tree_vierge")
    
    self.register_stmt(onset_anchor_tree_vierge)
    
    onset_anchor_tree_reduced = gcd_table(onset_anchor_tree_vierge).cte(name="onset_anchor_tree_reduced")

    self.register_stmt(onset_anchor_tree_reduced)
    
    self.insert = global_onset.insert().from_select(['id','num','den'], onset_anchor_tree_reduced)

def generate_ddl(global_onset, onset_referent, onset_anchor, local_onset) :
  OUT = []

  insert_stmt = _Insert(global_onset, onset_referent, onset_anchor, local_onset)

  del_stmt = _Delete(global_onset, onset_referent, onset_anchor, local_onset)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])\
            for action in ['INSERT', 'UPDATE', 'DELETE']\
            for table in [onset_referent, onset_anchor, local_onset]]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  #MANUAL_DDL = False
  MANUAL_DDL = True
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(
                          global_onset = Global_onset,
                          onset_referent = Onset_referent,
                          onset_anchor = Onset_anchor,
                          local_onset = Local_onset))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  '''
  for ddl in ddls :
    event.listen(ddl.table, 'after_create', DDL(ddl.instruction).\
        execute_if(dialect='sqlite'))
  '''

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  NS = 2
  NV = 4
  NN = 64

  CT = 0
  LOCAL_OR = 0
  for x in range(NS) :
    for y in range(NV) :
      for z in range(NN) :
        if z == 0 :
          LOCAL_OR = CT
        stmts.append((Local_onset, {'id' : CT, 'num' : z, 'den' : 1}))
        if y > 0 :
          if (z == 0) :
            REF = (x * NS) + ((y - 1) * NV) + (NN / 2)
            # middle of previous voice
            stmts.append((Onset_anchor, {'id' : CT, 'val' : REF}))
          stmts.append((Onset_referent, {'id' : CT, 'val' : LOCAL_OR}))
        CT += 1

  #stmt = _make_insert_statement(Onset_referent, Time_next, Onset_anchor)

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0], st[1], MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  print "***********"
  for row in conn.execute(select([Global_onset])).fetchall() :
    print row

  #stmt = _make_delete_statement(Onset_referent, Time_next, match = 68, onset_anchor_statement = True)
  #print stmt
  #conn.execute(stmt)
  '''
  manager.insert(conn, Onset_anchor, {'id' : 68, 'val' : 20}, MANUAL_DDL)

  #print "***********"
  #for row in conn.execute(select([Onset_referent])).fetchall() :
  #  print row
  #conn.execute(Duration.update().values(num=100, den=1).where(Duration.c.id == 4))
  
  #print "*************"
  print time.time() - NOW
  for row in conn.execute(select([Global_onset])).fetchall() :
    print row
  '''