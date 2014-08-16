'''
FIX THIS !!!!
need to take into account number of staff lines
'''
from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time

from functools import partial

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, name, ledger_line) :
    def where_clause_fn(id) :
      stmt = select([name.c.id]).where(and_(ledger_line.c.id == id, name.c.id == id, name.c.val == 'note'))
      return exists(stmt)
    DeleteStmt.__init__(self, ledger_line, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, staff_position, ledger_line) :
    InsertStmt.__init__(self)
    self.name = name
    self.staff_position = staff_position
    self.ledger_line = ledger_line
  def _generate_stmt(self, id) :
    #print "@@ON ID", id
    name = self.name
    staff_position = self.staff_position
    ledger_line = self.ledger_line

    note_to_ledger_line = select([
      name.c.id.label('id'),
      # 0.3 gets rounding right
      case([(func.abs(staff_position.c.val) >= 3.0, (staff_position.c.val / func.abs(staff_position.c.val)) * func.round(func.abs(staff_position.c.val) - 2.0 - 0.3))], else_=0)
    ]).where(name.c.val == 'note').\
    where(safe_eq_comp(name.c.id, id)).\
    where(staff_position.c.id == name.c.id).cte(name="note_to_ledger_line")

    self.register_stmt(note_to_ledger_line)

    self.insert = simple_insert(ledger_line, note_to_ledger_line)

def generate_ddl(name, staff_position, ledger_line) :
  OUT = []

  insert_stmt = _Insert(name, staff_position, ledger_line)

  del_stmt = _Delete(name, ledger_line)

  when = EasyWhen(name, staff_position)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, staff_position]]

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
                                     staff_position = Staff_position,
                                     ledger_line = Ledger_line))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  #bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  for x in range(20) :
    stmts.append((Name, {'id':x,'val':'note'}))
    stmts.append((Staff_position, {'id':x,'val':-5.0 + (x / 2.0)}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Ledger_line])).fetchall() :
    print row
