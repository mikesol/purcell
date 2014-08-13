from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, stem_direction) :
    def where_clause_fn(id) :
      return stem_direction.c.id == id
    DeleteStmt.__init__(self, stem_direction, where_clause_fn)

class _DeleteBeam(DeleteStmt) :
  def __init__(self, beam, stem_direction) :
    def where_clause_fn(id) :
      beam_me = beam.alias(name='beam_me')
      beam_other = beam.alias(name='beam_other')
      stmt = select([beam_other.c.id]).\
         where(and_(stem_direction.c.id == beam_other.c.id,
            beam_other.c.val == beam_me.c.val,
            beam_me.c.id == id))
      return exists(stmt)
    DeleteStmt.__init__(self, stem_direction, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, natural_stem_direction, beam, stem_direction, beam_specialize = False) :
    InsertStmt.__init__(self)
    self.natural_stem_direction = natural_stem_direction
    self.beam = beam
    self.stem_direction = stem_direction
    self.beam_specialize = beam_specialize

  def _generate_stmt(self, id) : 
    natural_stem_direction = self.natural_stem_direction
    beam = self.beam
    stem_direction = self.stem_direction
    beam_specialize = self.beam_specialize

    my_beam = select([
      beam.c.val.label('beam')
    ]).where(safe_eq_comp(beam.c.id, id)).cte(name = 'my_beam')
    
    self.register_stmt(my_beam)

    others_beamed_with_me = select([
      beam.c.id.label('id'),
      beam.c.val.label('val')
    ]).where(beam.c.val == my_beam.c.beam).\
        cte(name = 'others_beamed_with_me')

    self.register_stmt(others_beamed_with_me)

    prevailing_direction = select([
      func.sum(natural_stem_direction.c.val).label('val'),
    ]).where(natural_stem_direction.c.id == others_beamed_with_me.c.id).\
    cte(name="prevailing_direction")

    self.register_stmt(prevailing_direction)

    stem_direction_for_beams = select([
      others_beamed_with_me.c.id.label('id'),
      prevailing_direction.c.val.label('val'),
    ]).\
    cte(name="stem_direction_for_beams")

    self.register_stmt(stem_direction_for_beams)

    natural_stem_direction_to_stem_direction = (select([
      natural_stem_direction.c.id.label('id'),
      case([(stem_direction_for_beams.c.val != None,
          case([(stem_direction_for_beams.c.val > 0, 1)], else_ = -1))],
        else_ = natural_stem_direction.c.val).label('val')
    ]).select_from(natural_stem_direction.\
        outerjoin(stem_direction_for_beams,
                   onclause=natural_stem_direction.c.id == stem_direction_for_beams.c.id)).\
      where(safe_eq_comp(natural_stem_direction.c.id, id)).\
    cte(name='natural_stem_direction_to_stem_direction')\
      if not beam_specialize else \
    select([
      stem_direction_for_beams.c.id.label('id'),
      case([(stem_direction_for_beams.c.val > 0, 1)], else_ = -1).label('val')
    ]).\
    cte(name='natural_stem_direction_to_stem_direction'))

    self.register_stmt(natural_stem_direction_to_stem_direction)

    self.insert = simple_insert(stem_direction, natural_stem_direction_to_stem_direction)

def generate_ddl(natural_stem_direction, beam, stem_direction) :
  OUT = []

  insert_stmt = _Insert(natural_stem_direction, beam, stem_direction)
  del_stmt = _Delete(stem_direction)

  insert_stmt_beam = _Insert(natural_stem_direction, beam, stem_direction, beam_specialize=True)#_InsertBeam(natural_stem_direction, beam, stem_direction)
  del_stmt_beam = _DeleteBeam(beam, stem_direction)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [natural_stem_direction]]

  OUT += [DDL_unit(table, action, [del_stmt_beam], [insert_stmt_beam])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [beam]]

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

  manager = DDL_manager(generate_ddl(natural_stem_direction = Natural_stem_direction,
                                     beam = Beam,
                                     stem_direction = Stem_direction))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  for x in range(10) :
    stmts.append((Natural_stem_direction, {'id': x,'val': (x % 2) * 2 - 1}))
    if (x > 2) and (x < 9) :
      stmts.append((Beam, {'id':x,'val': 15}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Stem_direction])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
