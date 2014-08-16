from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, width) :
    def where_clause_fn(id) :
      return width.c.id == id
    DeleteStmt.__init__(self, width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, bar_thickness, width) :
    InsertStmt.__init__(self)
    self.bar_thickness = bar_thickness
    self.width = width

  def _generate_stmt(self, id) :
    bar_thickness = self.bar_thickness
    width = self.width
    
    bar_lines_to_widths = select([
      bar_thickness.c.id.label('id'),
      bar_thickness.c.val.label('val'),
    ]).where(safe_eq_comp(bar_thickness.c.id, id)).\
    cte(name='bar_lines_to_widths')

    self.register_stmt(bar_lines_to_widths)

    self.insert = simple_insert(width, bar_lines_to_widths)

def generate_ddl(bar_thickness, width) :
  OUT = []

  insert_stmt = _Insert(bar_thickness, width)

  del_stmt = _Delete(width)

  when = EasyWhen(bar_thickness)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [bar_thickness]]

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

  manager = DDL_manager(generate_ddl(bar_thickness = Bar_thickness,
                                     width = Width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []
  #stmts.append((Staff_space, {'id':10, 'val':1.0}))
  #stmts.append((N_lines, {'id':10, 'val':5}))

  #stmts.append((Name, {'id':0,'val':'bar_line'}))
  stmts.append((Bar_thickness, {'id':0,'val':0.20}))
  stmts.append((Bar_thickness, {'id':1,'val':0.25}))
  #stmts.append((Staff_symbol, {'id':0,'val':10}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Width])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
