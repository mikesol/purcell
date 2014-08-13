from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, natural_stem_direction) :
    def where_clause_fn(id) :
      return natural_stem_direction.c.id == id
    DeleteStmt.__init__(self, natural_stem_direction, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, staff_position, natural_stem_direction) :
    InsertStmt.__init__(self)
    self.staff_position = staff_position
    self.natural_stem_direction = natural_stem_direction

  def _generate_stmt(self, id) : 
    staff_position = self.staff_position
    natural_stem_direction = self.natural_stem_direction

    duration_log_to_natural_stem_directions = select([
      staff_position.c.id.label('id'),
      case([(staff_position.c.val > 0, -1)], else_ = 1).label('val')
    ]).\
      where(safe_eq_comp(staff_position.c.id, id)).\
    cte(name='duration_log_to_natural_stem_directions')

    self.register_stmt(duration_log_to_natural_stem_directions)

    self.insert = simple_insert(natural_stem_direction, duration_log_to_natural_stem_directions)

def generate_ddl(staff_position, natural_stem_direction) :
  OUT = []

  insert_stmt = _Insert(staff_position, natural_stem_direction)

  del_stmt = _Delete(natural_stem_direction)
  
  when = EasyWhen(staff_position)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [staff_position]]

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

  manager = DDL_manager(generate_ddl(staff_position = Staff_position,
                                     natural_stem_direction = Natural_stem_direction))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  for x in range(10) :
    stmts.append((Staff_position, {'id': x,'val': (x/2.0) - 2.5}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Natural_stem_direction])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
