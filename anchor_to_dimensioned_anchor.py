from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
from staff_transform import staff_transform

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, anchor) :
    def where_clause_fn(id) :
      return anchor.c.id == id
    DeleteStmt.__init__(self, anchor, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, anchor, anchor_dim) :
    InsertStmt.__init__(self)
    self.anchor = anchor
    self.anchor_dim = anchor_dim

  def _generate_stmt(self, id) :
    anchor = self.anchor
    anchor_dim = self.anchor_dim

    anchor_to_anchor_dim = select([
      anchor.c.id.label('id'),
      anchor.c.val.label('val')
    ])
    self.register_stmt(anchor_to_anchor_dim)

    self.insert = simple_insert(anchor_dim, anchor_to_anchor_dim)

def generate_ddl(anchor, anchor_dim) :
  OUT = []

  insert_stmt = _Insert(anchor, anchor_dim)

  del_stmt = _Delete(anchor_dim)
  
  #when = EasyWhen(name)
  when = None
  
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [anchor]]

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

  manager = DDL_manager(generate_ddl(anchor = Anchor, anchor_dim = Anchor_x))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  stmts.append((Anchor, {'id':0,'val': 1}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Anchor_x])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
