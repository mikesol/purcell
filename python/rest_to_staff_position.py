from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time
import bravura_tools
from staff_transform import staff_transform

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, name, staff_position) :
    def where_clause_fn(id) :
      stmt = select([name.c.id]).where(and_(staff_position.c.id == id, name.c.id == id, name.c.val == 'rest'))
      return exists(stmt)
    DeleteStmt.__init__(self, staff_position, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, staff_position) :
    InsertStmt.__init__(self)
    self.name = name
    self.staff_position = staff_position

  def _generate_stmt(self, id) :
    name = self.name
    staff_position = self.staff_position

    rest_to_staff_position = select([
      name.c.id.label('id'),
      literal(0).label('val')
    ]).where(name.c.val == 'rest').\
       where(safe_eq_comp(name.c.id, id)).\
    cte(name='rest_to_staff_position')

    self.register_stmt(rest_to_staff_position)

    self.insert = simple_insert(staff_position, rest_to_staff_position)

def generate_ddl(name, staff_position) :
  OUT = []

  insert_stmt = _Insert(name, staff_position)

  del_stmt = _Delete(name, staff_position)
  
  #when = EasyWhen(name)
  when = None
  
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name]]

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

  manager = DDL_manager(generate_ddl(name = Name,
                                     staff_position = Staff_position))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  stmts.append((Name, {'id':0,'val': 'rest'}))

  stmts.append((Name, {'id':1,'val': 'note'}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Staff_position])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
