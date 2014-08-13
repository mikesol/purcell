from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

_THICK = 0.16

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, line_stencil) :
    def where_clause_fn(id) :
      return and_(line_stencil.c.id == id, line_stencil.c.writer == 'stem_to_line_stencil')
    DeleteStmt.__init__(self, line_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, stem_x_offset, stem_end, line_stencil) :
    InsertStmt.__init__(self)
    self.stem_x_offset = stem_x_offset
    self.stem_end = stem_end
    self.line_stencil = line_stencil

  def _generate_stmt(self, id) : 
    stem_x_offset = self.stem_x_offset
    stem_end = self.stem_end
    line_stencil = self.line_stencil

    #x0 = (stem_x_offset.c.val + case([(stem_end.c.val > 0, 2 * _THICK)],else_=_THICK))
    #x0 = (stem_x_offset.c.val + _THICK)
    x0 = (stem_x_offset.c.val + case([(stem_end.c.val > 0, 0.25)],else_=0.12))

    
    stem_to_line_stencil = select([
      stem_x_offset.c.id.label('id'),
      literal('stem_to_line_stencil').label('writer'),
      literal(0).label('sub_id'),
      x0.label('x0'),
      literal(0.0).label('y0'),
      x0.label('x1'),
      (stem_end.c.val * -1).label('y1'),
      literal(_THICK).label('thickness')
    ]).\
      where(stem_x_offset.c.id == stem_end.c.id).\
      where(stem_end.c.id == stem_x_offset.c.id).\
      where(safe_eq_comp(stem_end.c.id, id)).\
    cte(name='stem_to_line_stencil')

    self.register_stmt(stem_to_line_stencil)

    self.insert = simple_insert(line_stencil, stem_to_line_stencil)

def generate_ddl(stem_x_offset, stem_end, line_stencil) :
  OUT = []

  insert_stmt = _Insert(stem_x_offset, stem_end, line_stencil)

  del_stmt = _Delete(line_stencil)

  when = EasyWhen(stem_x_offset, stem_end)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [stem_x_offset, stem_end]]

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

  manager = DDL_manager(generate_ddl(stem_x_offset = Stem_x_offset,
                                     stem_end = Stem_end,
                                     line_stencil = Line_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  stmts.append((Staff_space, {'id':10,'val':1.0}))

  for x in range(10) :
    stmts.append((Stem_x_offset, {'id':x,'val': 0.0 if x < 5 else 5.0}))
    stmts.append((Stem_end, {'id':x,'val': -3.5 if x < 5 else 3.5}))

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
