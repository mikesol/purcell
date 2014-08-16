from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, natural_stem_end) :
    def where_clause_fn(id) :
      return natural_stem_end.c.id == id
    DeleteStmt.__init__(self, natural_stem_end, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, stem_direction, stem_length, natural_stem_end) :
    InsertStmt.__init__(self)
    self.stem_direction = stem_direction
    self.stem_length = stem_length
    self.natural_stem_end = natural_stem_end

  def _generate_stmt(self, id) : 
    stem_direction = self.stem_direction
    stem_length = self.stem_length
    natural_stem_end = self.natural_stem_end

    stem_to_natural_stem_end = select([
      stem_direction.c.id.label('id'),
      (stem_direction.c.val * stem_length.c.val).label('val'),
    ]).\
      where(stem_length.c.id == stem_direction.c.id).\
      where(safe_eq_comp(stem_direction.c.id, id)).\
    cte(name='stem_to_natural_stem_end')

    self.register_stmt(stem_to_natural_stem_end)

    self.insert = simple_insert(natural_stem_end, stem_to_natural_stem_end)

def generate_ddl(stem_direction, stem_length, natural_stem_end) :
  OUT = []

  insert_stmt = _Insert(stem_direction, stem_length, natural_stem_end)

  del_stmt = _Delete(natural_stem_end)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [stem_direction, stem_length]]

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

  manager = DDL_manager(generate_ddl(stem_direction = Stem_direction,
                                     stem_length = Stem_length,
                                     natural_stem_end = Natural_stem_end))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  #stmts.append((Staff_space, {'id':10,'val':1.0}))

  for x in range(10) :
    stmts.append((Stem_direction, {'id': x,'val': -1 if x < 5 else 1}))
    stmts.append((Stem_length, {'id':x,'val': 3.5}))
    #stmts.append((Staff_symbol, {'id':x,'val': 10}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Natural_stem_end])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
