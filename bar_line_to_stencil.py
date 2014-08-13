from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, line_stencil) :
    def where_clause_fn(id) :
      return and_(line_stencil.c.id == id, line_stencil.c.writer == 'bar_line_to_stencil')
    DeleteStmt.__init__(self, line_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, bar_thickness, staff_symbol, staff_space, n_lines, line_stencil) :
    InsertStmt.__init__(self)
    self.name = name
    self.bar_thickness = bar_thickness
    self.staff_symbol = staff_symbol
    self.staff_space = staff_space
    self.n_lines = n_lines
    self.line_stencil = line_stencil

  def _generate_stmt(self, id) :
    name = self.name
    bar_thickness = self.bar_thickness
    staff_symbol = self.staff_symbol
    staff_space = self.staff_space
    n_lines = self.n_lines
    line_stencil = self.line_stencil
    
    bar_lines_to_stencils = select([
      name.c.id.label('id'),
      literal('bar_line_to_stencil').label('writer'),
      literal(0).label('sub_id'),
      literal(0.0).label('x0'),
      literal(0.0).label('y0'),
      literal(0.0).label('x1'),
      (staff_space.c.val * (n_lines.c.val - 1)).label('y1'),
      bar_thickness.c.val.label('thickness'),
    ]).where(and_(name.c.val == 'bar_line',
                  name.c.id == bar_thickness.c.id,
                  name.c.id == staff_symbol.c.id,
                  staff_symbol.c.val == staff_space.c.id,
                  staff_symbol.c.val == n_lines.c.id,
                  )).\
    where(safe_eq_comp(name.c.id, id)).\
    cte(name='bar_lines_to_stencils')

    self.register_stmt(bar_lines_to_stencils)

    self.insert = simple_insert(line_stencil, bar_lines_to_stencils)

def generate_ddl(name, bar_thickness, staff_symbol, staff_space, n_lines, line_stencil) :
  OUT = []

  insert_stmt = _Insert(name, bar_thickness, staff_symbol, staff_space, n_lines, line_stencil)

  del_stmt = _Delete(line_stencil)

  when = EasyWhen(name, bar_thickness, staff_symbol)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, bar_thickness, staff_symbol, staff_space]]

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
                                     bar_thickness = Bar_thickness,
                                     staff_symbol = Staff_symbol,
                                     staff_space = Staff_space,
                                     n_lines = N_lines,
                                     line_stencil = Line_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []
  stmts.append((Staff_space, {'id':10, 'val':1.0}))
  stmts.append((N_lines, {'id':10, 'val':5}))

  stmts.append((Name, {'id':0,'val':'bar_line'}))
  stmts.append((Bar_thickness, {'id':0,'val':0.20}))
  stmts.append((Staff_symbol, {'id':0,'val':10}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Line_stencil])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
