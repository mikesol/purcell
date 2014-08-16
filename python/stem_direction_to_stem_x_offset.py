from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, stem_x_offset) :
    def where_clause_fn(id) :
      return stem_x_offset.c.id == id
    DeleteStmt.__init__(self, stem_x_offset, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, rhythmic_head_width, stem_direction, stem_x_offset) :
    InsertStmt.__init__(self)
    self.rhythmic_head_width = rhythmic_head_width
    self.stem_direction = stem_direction
    self.stem_x_offset = stem_x_offset

  def _generate_stmt(self, id) : 
    rhythmic_head_width = self.rhythmic_head_width
    stem_direction = self.stem_direction
    stem_x_offset = self.stem_x_offset

    stem_direction_to_stem_x_offset = select([
      stem_direction.c.id.label('id'),
      case([(stem_direction.c.val > 0, rhythmic_head_width.c.val)], else_ = 0.0).label('val')
    ]).\
      where(stem_direction.c.id == rhythmic_head_width.c.id).\
      where(safe_eq_comp(stem_direction.c.id, id)).\
    cte(name='stem_direction_to_stem_x_offset')

    self.register_stmt(stem_direction_to_stem_x_offset)

    self.insert = simple_insert(stem_x_offset, stem_direction_to_stem_x_offset)

def generate_ddl(rhythmic_head_width, stem_direction, stem_x_offset) :
  OUT = []

  insert_stmt = _Insert(rhythmic_head_width, stem_direction, stem_x_offset)

  del_stmt = _Delete(stem_x_offset)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [rhythmic_head_width, stem_direction]]

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

  manager = DDL_manager(generate_ddl(rhythmic_head_width = Rhythmic_head_width,
                                     stem_direction = Stem_direction,
                                     stem_x_offset = Stem_x_offset))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  for x in range(10) :
    stmts.append((Stem_direction, {'id': x,'val': -1 if x < 5 else 1}))
    stmts.append((Rhythmic_head_width, {'id':x,'val': 5.0}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Stem_x_offset])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
