from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import bravura_tools
from staff_transform import staff_transform

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, alignment_directive) :
    def where_clause_fn(id) :
      return alignment_directive.c.id == id
    DeleteStmt.__init__(self, alignment_directive, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, dynamic_direction, alignment_directive) :
    InsertStmt.__init__(self)
    self.dynamic_direction = dynamic_direction
    self.alignment_directive = alignment_directive

  def _generate_stmt(self, id) :
    dynamic_direction = self.dynamic_direction
    alignment_directive = self.alignment_directive
    
    dynamic_to_alignment_directive = select([
      dynamic_direction.c.id.label('id'),
      ((dynamic_direction.c.val + 1.0) / 2.0).label('y')
    ]).where(safe_eq_comp(dynamic_direction.c.id, id)).\
    cte(name='dynamic_to_alignment_directive')

    self.register_stmt(dynamic_to_alignment_directive)

    self.insert = alignment_directive.insert().from_select(['id','y'],
    dynamic_to_alignment_directive)

def generate_ddl(dynamic_direction, alignment_directive) :
  OUT = []

  insert_stmt = _Insert(dynamic_direction, alignment_directive)

  del_stmt = _Delete(alignment_directive)
  
  when = None
  
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [dynamic_direction]]

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

  manager = DDL_manager(generate_ddl(
    dynamic_direction = Dynamic_direction,
    alignment_directive = Alignment_directive))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  for x in range(5) :
    stmts.append((Dynamic_direction, {'id':x, 'val':(x % 2) * 2 - 1}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Alignment_directive])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
