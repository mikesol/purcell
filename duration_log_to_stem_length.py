from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, stem_length) :
    def where_clause_fn(id) :
      return stem_length.c.id == id
    DeleteStmt.__init__(self, stem_length, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, duration_log, name, beam, stem_length) :
    InsertStmt.__init__(self)
    self.duration_log = duration_log
    self.name = name
    self.beam = beam
    self.stem_length = stem_length

  def _generate_stmt(self, id) : 
    duration_log = self.duration_log
    name = self.name
    beam = self.beam
    stem_length = self.stem_length

    duration_log_to_stem_lengths = select([
      duration_log.c.id.label('id'),
      # 0.75 magic number for beam and space...
      case([(beam.c.val != None, 4.0 + (0.75 * ((duration_log.c.val * -1) - 3.0)))], else_=4.0).label('val')
    ]).select_from(duration_log.outerjoin(beam, onclause = duration_log.c.id == beam.c.id)).\
      where(safe_eq_comp(duration_log.c.id, id)).\
      where(duration_log.c.val < 0).\
      where(name.c.id == duration_log.c.id).\
      where(name.c.val == 'note').\
          cte(name='duration_log_to_stem_lengths')

    self.register_stmt(duration_log_to_stem_lengths)

    self.insert = simple_insert(stem_length, duration_log_to_stem_lengths)

def generate_ddl(duration_log, name, beam, stem_length) :
  OUT = []

  insert_stmt = _Insert(duration_log, name, beam, stem_length)

  del_stmt = _Delete(stem_length)
  
  when = EasyWhen(duration_log, name)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [duration_log, name]]

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

  manager = DDL_manager(generate_ddl(duration_log = Duration_log,
                                     name = Name,
                                     beam = Beam,
                                     stem_length = Stem_length))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  DL = [-4,-4,-4,-4,-3,-2,-1,0]
  for n in range(len(DL)) :
    if n < 2 :
      stmts.append((Beam, {'id':n, 'val':100}))
    stmts.append((Duration_log, {'id': n,'val': DL[n]}))
    stmts.append((Name, {'id':n,'val': 'note'}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Stem_length])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
