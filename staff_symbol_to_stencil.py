from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import emmentaler_tools
import duration_log_to_dimension

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, line_stencil) :
    def where_clause_fn(id) :
      return line_stencil.c.id == id
    DeleteStmt.__init__(self, line_stencil, where_clause_fn)

class _Delete_XP(DeleteStmt) :
  def __init__(self, line_stencil) :
    def where_clause_fn(id) :
      # uggghh
      return line_stencil.c.id != 3.1416
    DeleteStmt.__init__(self, line_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, line_thickness, n_lines, staff_space, x_position, note_head_height, line_stencil) :
    InsertStmt.__init__(self)

    x_position_min_max = select([
      func.min(x_position.c.val).label('x_position_min'),
      func.max(x_position.c.val).label('x_position_max')
    ]).cte(name="x_position_min_max")

    self.register_stmt(x_position_min_max)
    
    first_line = select([
      name.c.id.label('id'),
      literal(0).label('sub_id'),
      x_position_min_max.c.x_position_min.label('x0'),
      literal(0.0).label('y0'),
      x_position_min_max.c.x_position_max.label('x1'),
      literal(0.0).label('y1'),
      line_thickness.c.val.label('thickness'),
    ]).select_from(
        name.\
        join(line_thickness, onclause = name.c.id == line_thickness.c.id).\
        join(n_lines, onclause = name.c.id == n_lines.c.id).\
        join(staff_space, onclause = name.c.id == staff_space.c.id).\
        outerjoin(line_stencil, onclause = name.c.id == line_stencil.c.id)
      ).where(name.c.val == "staff_symbol").\
        where(x_position_min_max.c.x_position_min != None).\
        where(x_position_min_max.c.x_position_max != None).\
        where(line_stencil.c.sub_id == None).\
        where(n_lines.c.val > 0).cte(name = 'line_from_staff_symbol', recursive = True)

    self.register_stmt(first_line)

    prev_line = first_line.alias(name="prev_line")

    # -101 is the height of a quarter note
    to_insert = first_line.union_all(
      select([
        prev_line.c.id,
        prev_line.c.sub_id + 1,
        x_position_min_max.c.x_position_min,
        prev_line.c.y0 + (staff_space.c.val * note_head_height.c.val),
        x_position_min_max.c.x_position_max,
        prev_line.c.y1 + (staff_space.c.val * note_head_height.c.val),
        prev_line.c.thickness
      ]).select_from(prev_line.join(line_thickness, onclause = prev_line.c.id == line_thickness.c.id).\
        join(n_lines, onclause = prev_line.c.id == n_lines.c.id).\
        join(staff_space, onclause = prev_line.c.id == staff_space.c.id)
      ).where(and_(prev_line.c.sub_id + 1 < n_lines.c.val,
                    note_head_height.c.id == -101))
    )

    self.register_stmt(to_insert)

    self.insert = simple_insert(line_stencil, to_insert)

def generate_ddl(name, line_thickness, n_lines, staff_space, x_position, note_head_height, line_stencil) :
  OUT = []

  insert_stmt = _Insert(name, line_thickness, n_lines, staff_space, x_position, note_head_height, line_stencil)

  del_stmt = _Delete(line_stencil)
  del_stmt_xp = _Delete_XP(line_stencil)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, line_thickness, n_lines, staff_space]]

  OUT += [DDL_unit(table, action, [del_stmt_xp], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [x_position]]

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
                                     line_thickness = Line_thickness,
                                     n_lines = N_lines,
                                     staff_space = Staff_space,
                                     x_position = X_position,
                                     note_head_height = Note_head_height,
                                     line_stencil = Line_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  emmentaler_tools.populate_glyph_box_table(conn, Glyph_box, from_cache=True)
  conn.execute(duration_log_to_dimension.initialize_dimensions_of_quarter_note(Glyph_box, Note_head_width, 'width'))
  conn.execute(duration_log_to_dimension.initialize_dimensions_of_quarter_note(Glyph_box, Note_head_height, 'height'))

  #print duration_log_to_dimension.initialize_dimensions_of_quarter_note(Glyph_box, Note_head_height, 'height')
  #for row in conn.execute(select([Note_head_width])).fetchall() : print row
  #for row in conn.execute(select([Note_head_height])).fetchall() : print row

  stmts = []

  stmts.append((Name, {'id':0,'val':'staff_symbol'}))
  stmts.append((Line_thickness, {'id':0, 'val':0.1}))
  stmts.append((N_lines, {'id':0,'val':5}))
  stmts.append((Staff_space, {'id':0, 'val':1.0}))
  XP = [0.2,0.1,0.0,1000.0]
  for x in range(len(XP)) :
    stmts.append((X_position, {'id': x + 1, 'val': XP[x]}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Line_stencil])).fetchall() :
    print row
