from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import bravura_tools
from staff_transform import staff_transform

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, dynamic, staff_position) :
    def where_clause_fn(id) :
      stmt = select([dynamic.c.id]).where(and_(staff_position.c.id == id, dynamic.c.id == id))
      return exists(stmt)
    DeleteStmt.__init__(self, staff_position, where_clause_fn)

class _DeleteAnchor_x(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, dynamic, anchor_x, staff_position) :
    def where_clause_fn(id) :
      stmt = select([dynamic.c.id]).where(and_(staff_position.c.id == dynamic.c.id, dynamic.c.id == anchor_x.c.id, anchor_x.c.val == id))
      return exists(stmt)
    DeleteStmt.__init__(self, staff_position, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, dynamic, anchor_x, note_box, dynamic_direction, dynamic_padding, staff_position, from_anchor_x) :
    InsertStmt.__init__(self)
    self.dynamic = dynamic
    self.anchor_x = anchor_x
    self.note_box = note_box
    self.dynamic_direction = dynamic_direction
    self.dynamic_padding = dynamic_padding
    self.staff_position = staff_position
    self.from_anchor_x = from_anchor_x

  def _generate_stmt(self, id) :
    dynamic = self.dynamic
    anchor_x = self.anchor_x
    note_box = self.note_box
    dynamic_direction = self.dynamic_direction
    dynamic_padding = self.dynamic_padding
    staff_position = self.staff_position
    from_anchor_x = self.from_anchor_x
    
    dynamic_padding_default = dynamic_padding.alias(name='dynamic_padding_default')

    dynamic_to_staff_position = select([
      dynamic.c.id.label('id'),
      # ughh...2.0 magic number for staff
      ((dynamic_direction.c.val * case([(dynamic_padding.c.val != None, dynamic_padding.c.val)], else_=dynamic_padding_default.c.val)) +
        case([(dynamic_direction.c.val == 1,
               sql_min_max([note_box.c.y + note_box.c.height, 2.0], True)),
            (dynamic_direction.c.val == -1,
               sql_min_max([note_box.c.y, -2.0], False))])).label('val')
    ]).select_from(dynamic.outerjoin(dynamic_padding, onclause = dynamic.c.id == dynamic_padding.c.id)).\
       where(safe_eq_comp(note_box.c.id if from_anchor_x else dynamic.c.id, id)).\
       where(anchor_x.c.id == dynamic.c.id).\
       where(note_box.c.id == anchor_x.c.val).\
       where(dynamic_padding_default.c.id == -1).\
       where(dynamic.c.id == dynamic_direction.c.id).\
    cte(name='dynamic_to_staff_position')

    self.register_stmt(dynamic_to_staff_position)

    self.insert = simple_insert(staff_position, dynamic_to_staff_position)

def generate_ddl(dynamic, anchor_x, note_box, dynamic_direction, dynamic_padding, staff_position) :
  OUT = []

  insert_stmt = _Insert(dynamic, anchor_x, note_box, dynamic_direction, dynamic_padding, staff_position, from_anchor_x=False)
  insert_stmt_anchor_x = _Insert(dynamic, anchor_x, note_box, dynamic_direction, dynamic_padding, staff_position, from_anchor_x=True)

  del_stmt = _Delete(dynamic, staff_position)
  del_stmt_anchor_x = _DeleteAnchor_x(dynamic, anchor_x, staff_position)
  
  when = EasyWhen(dynamic, dynamic_direction)
  
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [dynamic, dynamic_direction, dynamic_padding]]

  OUT += [DDL_unit(table, action, [del_stmt_anchor_x], [insert_stmt_anchor_x], when_clause = None)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [note_box]]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  MANUAL_DDL = True
  #MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(dynamic = Dynamic,
    anchor_x = Anchor_x,
    note_box = Note_box,
    dynamic_direction = Dynamic_direction,
    dynamic_padding = Dynamic_padding,
    staff_position = Staff_position))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []
  stmts.append((Dynamic_padding, {'id':-1, 'val': 0.3}))

  for x in range(5) :
    stmts.append((Dynamic, {'id':x, 'val':"U+foo"}))
    stmts.append((Anchor_x, {'id':x, 'val':x+5}))
    stmts.append((Note_box, {'id':x+5, 'x':0.0,'y':0.0,'width':1.0,'height':2.0}))
    stmts.append((Dynamic_direction, {'id':x, 'val':(x % 2) * 2 - 1}))

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
