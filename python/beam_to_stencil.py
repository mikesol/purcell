from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time
import staff_transform

_THICK = 0.16

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, polygon_stencil) :
    def where_clause_fn(id) :
      return and_(polygon_stencil.c.id == id, polygon_stencil.c.writer == 'beam_to_stencil')
    DeleteStmt.__init__(self, polygon_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, duration_log, stem_direction, beam, beam_x_position, beam_y_position, polygon_stencil) :
    InsertStmt.__init__(self)
    self.duration_log = duration_log
    self.beam = beam
    self.stem_direction = stem_direction
    self.beam_x_position = beam_x_position
    self.beam_y_position = beam_y_position
    self.polygon_stencil = polygon_stencil

  def _generate_stmt(self, id) :
    duration_log = self.duration_log
    beam = self.beam
    stem_direction = self.stem_direction
    beam_x_position = self.beam_x_position
    beam_y_position = self.beam_y_position
    polygon_stencil = self.polygon_stencil

    beam_direction = select([
      # should all be the same
      func.min(stem_direction.c.val).label('val')
    ]).where(safe_eq_comp(beam.c.val, id)).\
    where(beam.c.id == stem_direction.c.id).\
    cte(name="beam_direction")

    # ugggghhhh
    beam_duration_log = select([
      # should all be the same
      func.min(duration_log.c.val).label('val')
    ]).where(safe_eq_comp(beam.c.val, id)).\
    where(beam.c.id == duration_log.c.id).\
    cte(name="beam_duration_log")

    # two levels of recursion
    # - one for the number of beams
    # - one for the polygon

    # ugh...y is for now left/right
    beam_starting_points = select([
      beam_x_position.c.id.label('id'),
      literal(0).label('sub_id'),
      beam_x_position.c.left.label('x'),
      beam_y_position.c.left.label('y'),
    ]).where(safe_eq_comp(beam_x_position.c.id, id)).\
      where(beam_x_position.c.id == beam_y_position.c.id).\
    cte(name = 'beam_starting_points', recursive = True)
    
    self.register_stmt(beam_starting_points)

    beam_starting_points_prev = beam_starting_points.alias(name='beam_starting_points_prev')
    
    beam_starting_points = beam_starting_points.union_all(
      select([
        beam_starting_points_prev.c.id,
        (beam_starting_points_prev.c.sub_id + 1),
        beam_starting_points_prev.c.x,
        (beam_starting_points_prev.c.y +\
           (beam_direction.c.val *\
              (beam_starting_points_prev.c.sub_id + 1) * -0.75)),
      ]).\
      where(beam_starting_points_prev.c.sub_id < ((beam_duration_log.c.val * -1) - 2) - 1)
    )

    self.register_stmt(beam_starting_points)

    beam_polygons_1 = select([
      beam_starting_points.c.id.label('id'),
      literal('beam_to_stencil').label('writer'),
      beam_starting_points.c.sub_id.label('sub_id'),
      literal(0).label('point'),
      (beam_starting_points.c.x + _THICK).label('x'),
      beam_starting_points.c.y.label('y'),
      literal(0).label('thickness'),
      literal(1).label('fill'),
      literal(0).label('stroke')
    ]).cte(name = 'beam_polygons_1')

    self.register_stmt(beam_polygons_1)
    
    beam_polygons_2 = select([
        beam_polygons_1.c.id,
        beam_polygons_1.c.writer,
        beam_polygons_1.c.sub_id,
        beam_polygons_1.c.point,
        beam_polygons_1.c.x,
        beam_polygons_1.c.y,
        beam_polygons_1.c.thickness,
        beam_polygons_1.c.fill,
        beam_polygons_1.c.stroke
    ]).union_all(
      select([
        beam_polygons_1.c.id,
        beam_polygons_1.c.writer,
        beam_polygons_1.c.sub_id,
        literal(1),
        (beam_polygons_1.c.x + _THICK),
        beam_polygons_1.c.y - 0.5,
        beam_polygons_1.c.thickness,
        beam_polygons_1.c.fill,
        beam_polygons_1.c.stroke
      ])
    ).cte(name = "beam_polygons_2")

    self.register_stmt(beam_polygons_2)

    beam_polygons_3 = select([
        beam_polygons_2.c.id,
        beam_polygons_2.c.writer,
        beam_polygons_2.c.sub_id,
        beam_polygons_2.c.point,
        beam_polygons_2.c.x,
        beam_polygons_2.c.y,
        beam_polygons_2.c.thickness,
        beam_polygons_2.c.fill,
        beam_polygons_2.c.stroke
    ]).union_all(
      select([
        beam_polygons_1.c.id,
        beam_polygons_1.c.writer,
        beam_polygons_1.c.sub_id,
        literal(2),
        # small overshoot to cover
        (beam_x_position.c.right + 0.1 + _THICK),
        beam_polygons_1.c.y - beam_y_position.c.left + beam_y_position.c.right - 0.5,
        beam_polygons_1.c.thickness,
        beam_polygons_1.c.fill,
        beam_polygons_1.c.stroke
      ]).where(beam_x_position.c.id == beam_polygons_1.c.id).\
          where(beam_y_position.c.id == beam_polygons_1.c.id)
    ).cte(name = "beam_polygons_3")

    self.register_stmt(beam_polygons_3)

    beam_polygons_4 = select([
        beam_polygons_3.c.id,
        beam_polygons_3.c.writer,
        beam_polygons_3.c.sub_id,
        beam_polygons_3.c.point,
        beam_polygons_3.c.x,
        beam_polygons_3.c.y,
        beam_polygons_3.c.thickness,
        beam_polygons_3.c.fill,
        beam_polygons_3.c.stroke
    ]).union_all(
      select([
        beam_polygons_1.c.id,
        beam_polygons_1.c.writer,
        beam_polygons_1.c.sub_id,
        literal(3),
        (beam_x_position.c.right + 0.1 + _THICK),
        beam_polygons_1.c.y - beam_y_position.c.left + beam_y_position.c.right,
        beam_polygons_1.c.thickness,
        beam_polygons_1.c.fill,
        beam_polygons_1.c.stroke
      ]).where(beam_x_position.c.id == beam_polygons_1.c.id).\
        where(beam_y_position.c.id == beam_polygons_1.c.id)
    ).cte(name = "beam_polygons_4")

    self.register_stmt(beam_polygons_4)

    beam_polygons_5 = select([
        beam_polygons_4.c.id,
        beam_polygons_4.c.writer,
        beam_polygons_4.c.sub_id,
        beam_polygons_4.c.point,
        beam_polygons_4.c.x,
        beam_polygons_4.c.y,
        beam_polygons_4.c.thickness,
        beam_polygons_4.c.fill,
        beam_polygons_4.c.stroke
    ]).union_all(
      select([
        beam_polygons_1.c.id,
        beam_polygons_1.c.writer,
        beam_polygons_1.c.sub_id,
        literal(4),
        beam_polygons_1.c.x + _THICK,
        beam_polygons_1.c.y,
        beam_polygons_1.c.thickness,
        beam_polygons_1.c.fill,
        beam_polygons_1.c.stroke
      ])
    ).cte(name = "beam_polygons_5")

    self.register_stmt(beam_polygons_5)

    beam_polygons_kludgified_for_staff = select([
        beam_polygons_5.c.id,
        beam_polygons_5.c.writer,
        beam_polygons_5.c.sub_id,
        beam_polygons_5.c.point,
        beam_polygons_5.c.x,
        staff_transform.staff_transform(beam_polygons_5.c.y),
        beam_polygons_5.c.thickness,
        beam_polygons_5.c.fill,
        beam_polygons_5.c.stroke
    ]).cte(name = "beam_polygons_kludgified_for_staff")

    self.register_stmt(beam_polygons_kludgified_for_staff)

    self.insert = simple_insert(polygon_stencil, beam_polygons_kludgified_for_staff)

def generate_ddl(duration_log, stem_direction, beam, beam_x_position, beam_y_position, polygon_stencil) :
  OUT = []

  insert_stmt = _Insert(duration_log, stem_direction, beam, beam_x_position, beam_y_position, polygon_stencil)
  del_stmt = _Delete(polygon_stencil)

  # ugggh...maybe need to add more tables
  # this should cover it all, tho...except changes in duration
  # need to have a stem duration thing
  # music is complicated!!
  
  when = EasyWhen(beam_x_position, beam_y_position)
  
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
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

  manager = DDL_manager(generate_ddl(duration_log = Duration_log,
                                     stem_direction = Stem_direction,
                                     beam = Beam,
                                     beam_x_position = Beam_x_position,
                                     beam_y_position = Beam_y_position,
                                     polygon_stencil = Polygon_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  for x in range(10) :
    stmts.append((Duration_log, {'id':x, 'val': -4}))
    stmts.append((Beam, {'id':x, 'val': 10 if x < 4 else 11}))
    stmts.append((Stem_direction, {'id':x, 'val': -1 if x < 4 else 1}))

  for x in range(10,12) :
    stmts.append((Beam_x_position, {'id':x,'left':x * 4.0, 'right': x * 4.3}))
    stmts.append((Beam_y_position, {'id':x,'left': x / 3.0,'right': x / 4.0}))
    
  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Polygon_stencil])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
