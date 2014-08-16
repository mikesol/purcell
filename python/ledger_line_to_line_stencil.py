'''
FIX THIS !!!!
normalize staff position correctly...
'''
from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time
#import bravura_tools

from functools import partial

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, name, line_stencil) :
    def where_clause_fn(id) :
      # ugggh... > 0 for reserved stem line...scrub this, use more tables in the future
      #stmt = select([name.c.id]).where(and_(line_stencil.c.id == id, name.c.id == id, name.c.val == 'note', line_stencil.c.sub_id > 0))
      #return exists(stmt)
      return and_(line_stencil.c.id == id, line_stencil.c.writer == 'ledger_line_to_line_stencil')
    DeleteStmt.__init__(self, line_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, ledger_line, n_lines, staff_space, staff_symbol, rhythmic_head_width, y_position, line_stencil) :
    InsertStmt.__init__(self)
    self.ledger_line = ledger_line
    self.n_lines = n_lines
    self.staff_space = staff_space
    self.staff_symbol = staff_symbol
    self.rhythmic_head_width = rhythmic_head_width
    self.y_position = y_position
    self.line_stencil = line_stencil
  def _generate_stmt(self, id) :
    #print "@@ON ID", id
    ## ugggh for y_position
    ledger_line = self.ledger_line
    n_lines = self.n_lines
    staff_space = self.staff_space
    staff_symbol = self.staff_symbol
    rhythmic_head_width = self.rhythmic_head_width
    y_position = self.y_position
    line_stencil = self.line_stencil

    ledger_line_to_line_stencil = select([
      ledger_line.c.id.label('id'),
      literal('ledger_line_to_line_stencil').label('writer'),
      literal(0).label('sub_id'),
      literal(-0.6).label('x0'),
      (case([(ledger_line.c.val < 0, staff_space.c.val * n_lines.c.val)], else_ = - staff_space.c.val) - y_position.c.val).label('y0'),
      (rhythmic_head_width.c.val + 1.0).label('x1'),
      (case([(ledger_line.c.val < 0, staff_space.c.val * n_lines.c.val)], else_ = - staff_space.c.val) - y_position.c.val).label('y1'),
      literal(0.13).label('thickness')
    ]).\
    where(safe_eq_comp(ledger_line.c.id, id)).\
    where(func.abs(ledger_line.c.val) > 0).\
    where(n_lines.c.id == staff_symbol.c.val).\
    where(staff_space.c.id == staff_symbol.c.val).\
    where(y_position.c.id == ledger_line.c.id).\
    where(staff_symbol.c.id == ledger_line.c.id).\
    where(rhythmic_head_width.c.id == staff_symbol.c.id).\
    cte(name="ledger_line_to_line_stencil", recursive = True)

    #where(safe_eq_comp(ledger_line.c.id, id))

    self.register_stmt(ledger_line_to_line_stencil)

    ledger_line_to_line_stencil_prev = ledger_line_to_line_stencil.alias(name="ledger_line_to_line_stencil_prev")

    ledger_line_to_line_stencil = ledger_line_to_line_stencil.union_all(
     select([
       ledger_line_to_line_stencil_prev.c.id,
       literal('ledger_line_to_line_stencil'),
       ledger_line_to_line_stencil_prev.c.sub_id + 1,
       ledger_line_to_line_stencil_prev.c.x0,
       ledger_line_to_line_stencil_prev.c.y0 + (staff_space.c.val * - 1.0 * ledger_line.c.val / func.abs(ledger_line.c.val)),
       ledger_line_to_line_stencil_prev.c.x1,
       ledger_line_to_line_stencil_prev.c.y1 + (staff_space.c.val * -1.0 * ledger_line.c.val / func.abs(ledger_line.c.val)),
       ledger_line_to_line_stencil_prev.c.thickness
     ]).\
     where(staff_space.c.id == staff_symbol.c.val).\
     where(staff_symbol.c.id == ledger_line_to_line_stencil_prev.c.id).\
     where(ledger_line_to_line_stencil_prev.c.id == ledger_line.c.id).\
     where(ledger_line_to_line_stencil_prev.c.sub_id < func.abs(ledger_line.c.val) - 1)
    )

    self.register_stmt(ledger_line_to_line_stencil)

    self.insert = simple_insert(line_stencil, ledger_line_to_line_stencil)

def generate_ddl(name, ledger_line, n_lines, staff_space, staff_symbol, rhythmic_head_width, y_position, line_stencil) :
  OUT = []

  insert_stmt = _Insert(ledger_line, n_lines, staff_space, staff_symbol, rhythmic_head_width, y_position, line_stencil)

  del_stmt = _Delete(name, line_stencil)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [ledger_line, rhythmic_head_width, staff_symbol, y_position]]

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

  manager = DDL_manager(generate_ddl(name = Name,
                                     ledger_line = Ledger_line,
                                     n_lines = N_lines,
                                     staff_space = Staff_space,
                                     staff_symbol = Staff_symbol,
                                     rhythmic_head_width = Rhythmic_head_width,
                                     y_position = Y_position,
                                     line_stencil = Line_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  stmts.append((N_lines, {'id':0,'val':5}))
  stmts.append((Staff_space, {'id':0,'val':1.0}))

  for x in range(1,20) :
    stmts.append((Name, {'id':x,'val':'note'}))
    stmts.append((Ledger_line, {'id':x,'val':x - 10}))
    stmts.append((Staff_symbol, {'id':x,'val':0}))
    stmts.append((Y_position, {'id':x,'val':x - 10}))
    stmts.append((Rhythmic_head_width, {'id':x,'val':1.34}))
    
  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Line_stencil])).fetchall() :
    print row
