from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# need to find a way to work font size into this...

class _DeleteFromStem(DeleteStmt) :
  def __init__(self, stem_end) :
    def where_clause_fn(id) :
      return stem_end.c.id == id
    DeleteStmt.__init__(self, stem_end, where_clause_fn)

class _DeleteFromBeam(DeleteStmt) :
  def __init__(self, beam, stem_end) :
    def where_clause_fn(id) :
      stmt = select([beam.c.val]).\
         where(and_(stem_end.c.id == beam.c.id,
            beam.c.val == id))
      return exists(stmt)
    DeleteStmt.__init__(self, stem_end, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, natural_stem_end, beam, beam_x_position, beam_y_position, x_position, stem_x_offset, staff_position, stem_end, id_is_beam = False) :
    InsertStmt.__init__(self)
    self.natural_stem_end = natural_stem_end
    self.beam = beam
    self.beam_x_position = beam_x_position
    self.beam_y_position = beam_y_position
    self.stem_x_offset = stem_x_offset
    self.x_position = x_position
    self.staff_position = staff_position
    self.stem_end = stem_end
    self.id_is_beam = id_is_beam

  def _generate_stmt(self, id) : 
    natural_stem_end = self.natural_stem_end
    beam = self.beam
    beam_x_position = self.beam_x_position
    beam_y_position = self.beam_y_position
    stem_x_offset = self.stem_x_offset
    x_position = self.x_position
    staff_position = self.staff_position
    stem_end = self.stem_end
    id_is_beam = self.id_is_beam

    # should only be one, so we do min...
    my_beam = select([
      func.min(beam.c.val).label('beam')
    ]).where(safe_eq_comp(beam.c.val if id_is_beam else beam.c.id, id)).\
      cte(name = 'my_beam')

    self.register_stmt(my_beam)

    slope = select([
      my_beam.c.beam.label('beam'),
      ((beam_y_position.c.right - beam_y_position.c.left) / (beam_x_position.c.right - beam_x_position.c.left)).label('slope')
    ]).where(my_beam.c.beam == beam_x_position.c.id).\
      where(my_beam.c.beam == beam_y_position.c.id).\
        cte(name = 'slope')
    
    self.register_stmt(slope)

    slope_offset = select([
      slope.c.beam.label('beam'),
      slope.c.slope.label('slope'),
      (beam_y_position.c.left - (slope.c.slope * beam_x_position.c.left)).label('offset')
    ]).cte(name="slope_offset")

    self.register_stmt(slope_offset)

    natural_stem_end_to_stem_end = select([
      natural_stem_end.c.id.label('id'),
      case([(slope_offset.c.slope != None, (slope_offset.c.slope * (x_position.c.val + stem_x_offset.c.val)) + slope_offset.c.offset - staff_position.c.val)], else_ = natural_stem_end.c.val).label('val'),
    ]).select_from(natural_stem_end.outerjoin(beam, onclause=beam.c.id==natural_stem_end.c.id).outerjoin(slope_offset, onclause = slope_offset.c.beam == beam.c.val)).\
       where(natural_stem_end.c.id == x_position.c.id).\
       where(staff_position.c.id == x_position.c.id).\
       where(natural_stem_end.c.id == stem_x_offset.c.id)

    if id_is_beam :
      natural_stem_end_to_stem_end = natural_stem_end_to_stem_end.\
        where(natural_stem_end.c.id == beam.c.id).\
        where(safe_eq_comp(beam.c.val, id)).cte(name='natural_stem_end_to_stem_end')
    else :
      natural_stem_end_to_stem_end = natural_stem_end_to_stem_end.\
        where(safe_eq_comp(natural_stem_end.c.id, id)).\
      cte(name='natural_stem_end_to_stem_end')

    self.register_stmt(natural_stem_end_to_stem_end)

    giant_kludge = select([stem_end]).cte(name='giant_kludge')
    self.register_stmt(giant_kludge)

    self.insert = simple_insert(stem_end, natural_stem_end_to_stem_end)

def generate_ddl(natural_stem_end, beam, beam_x_position, beam_y_position, x_position, stem_x_offset, staff_position, stem_end) :
  OUT = []

  insert_stmt = _Insert(natural_stem_end, beam, beam_x_position, beam_y_position, x_position, stem_x_offset, staff_position, stem_end, id_is_beam = False)
  insert_stmt_beam = _Insert(natural_stem_end, beam, beam_x_position, beam_y_position, x_position, stem_x_offset, staff_position, stem_end, id_is_beam = True)

  del_stmt = _DeleteFromStem(stem_end)
  del_stmt_beam = _DeleteFromBeam(beam, stem_end)

  # uggghh...way too many dependencies...need to find a way to
  # thin this out automatically via the manager
  # add dynamics, but then fix this
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [natural_stem_end, beam, stem_x_offset, x_position, staff_position]]

  OUT += [DDL_unit(table, action, [del_stmt_beam], [insert_stmt_beam])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [beam_x_position, beam_y_position]]

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

  manager = DDL_manager(generate_ddl(natural_stem_end = Natural_stem_end,
                                     beam = Beam,
                                     beam_x_position = Beam_x_position,
                                     beam_y_position = Beam_y_position,
                                     x_position = X_position,
                                     stem_x_offset = Stem_x_offset,
                                     staff_position = Staff_position,
                                     stem_end = Stem_end))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  #stmts.append((Staff_space, {'id':10,'val':1.0}))

  SE = [1.0,2.0,1.0,3.0,4.0,7.0]

  for x in range(6) :
    stmts.append((X_position, {'id':x, 'val':x * 1.5}))
    stmts.append((Stem_x_offset, {'id':x,'val':0.2}))
    stmts.append((Staff_position, {'id': x,'val': SE[x]}))
    stmts.append((Natural_stem_end, {'id': x,'val': 3.5}))
    if x > 2 :
      stmts.append((Beam, {'id':x,'val': 1000}))

  stmts.append((Beam_x_position, {'id':1000,'left':4.5,'right':7.5}))
  stmts.append((Beam_y_position, {'id':1000,'left':8.5,'right':10.5}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Stem_end])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
