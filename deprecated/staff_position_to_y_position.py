from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import bravura_tools
from staff_transform import staff_transform

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, needle, name, y_position) :
    def where_clause_fn(id) :
      stmt = select([name.c.id]).where(and_(y_position.c.id == id, name.c.id == id, name.c.val == needle))
      return exists(stmt)
    DeleteStmt.__init__(self, y_position, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, needle, name, staff_position, staff_symbol, staff_space, y_position) :
    InsertStmt.__init__(self)
    self.needle = needle
    self.name = name
    self.staff_position = staff_position
    self.staff_symbol = staff_symbol
    self.staff_space = staff_space
    self.y_position = y_position

  def _generate_stmt(self, id) :
    needle = self.needle
    name = self.name
    staff_position = self.staff_position
    staff_symbol = self.staff_symbol
    staff_space = self.staff_space
    y_position = self.y_position

    clefs_to_y_positions = select([
      name.c.id.label('id'),
      (staff_transform(staff_position.c.val) * staff_space.c.val).label('val')
    ]).where(and_(name.c.val == needle,
                  name.c.id == staff_position.c.id)).\
       where(staff_spaceize(name, staff_symbol, staff_space)).\
       where(safe_eq_comp(name.c.id, id)).\
    cte(name='clefs_to_y_positions')

    self.register_stmt(clefs_to_y_positions)

    self.insert = simple_insert(y_position, clefs_to_y_positions)

def generate_ddl(needle, name, staff_position, staff_symbol, staff_space, y_position) :
  OUT = []

  insert_stmt = _Insert(needle, name, staff_position, staff_symbol, staff_space, y_position)

  del_stmt = _Delete(needle, name, y_position)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, staff_position, staff_symbol]]

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

  manager = DDL_manager(generate_ddl(needle = 'clef',
                                     name = Name,
                                     staff_position = Staff_position,
                                     staff_symbol = Staff_symbol,
                                     staff_space = Staff_space,
                                     y_position = Y_position))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  stmts.append((Line_thickness, {'id':5, 'val':0.1}))
  stmts.append((Staff_space, {'id':5, 'val':1.0}))

  stmts.append((Staff_symbol, {'id':0,'val': 5}))
  stmts.append((Name, {'id':0,'val':'clef'}))
  stmts.append((Staff_position, {'id':0,'val':-1}))

  stmts.append((Staff_symbol, {'id':1,'val': 5}))
  stmts.append((Name, {'id':1,'val':'clef'}))
  stmts.append((Staff_position, {'id':1,'val':1}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Y_position])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row