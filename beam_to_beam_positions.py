from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, beam, beam_position) :
    def where_clause_fn(id) :
      stmt = select([beam.c.id]).\
         where(and_(beam_position.c.id == beam.c.val,
            beam.c.id == id))
      return exists(stmt)
    DeleteStmt.__init__(self, beam_position, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, stem_direction, natural_stem_end, staff_position, beam, x_position, stem_x_offset, beam_position, x_pos = True) :
    InsertStmt.__init__(self)
    self.stem_direction = stem_direction
    self.natural_stem_end = natural_stem_end
    self.staff_position = staff_position
    self.beam = beam
    self.x_position = x_position
    self.beam_position = beam_position
    self.stem_x_offset = stem_x_offset
    self.x_pos = x_pos

  def _generate_stmt(self, id) : 
    stem_direction = self.stem_direction
    natural_stem_end = self.natural_stem_end
    staff_position = self.staff_position
    beam = self.beam
    x_position = self.x_position
    beam_position = self.beam_position
    stem_x_offset = self.stem_x_offset
    x_pos = self.x_pos

    #natural_stem_end_left = natural_stem_end.alias(name = "natural_stem_end_left")
    #natural_stem_end_right = natural_stem_end.alias(name = "natural_stem_end_right")

    #staff_position_left = staff_position.alias(name = "staff_position_left")
    #staff_position_right = staff_position.alias(name = "staff_position_right")

    #x_position_left = x_position.alias(name = "x_position_left")
    #x_position_right = x_position.alias(name = "x_position_right")

    #stem_x_offset_left = stem_x_offset.alias(name = "stem_x_offset_left")
    #stem_x_offset_right = stem_x_offset.alias(name = "stem_x_offset_right")

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

    stem_x_positions = select([
      others_beamed_with_me.c.id.label('id'),
      (x_position.c.val + stem_x_offset.c.val).label('val'),
    ]).where(x_position.c.id == others_beamed_with_me.c.id).\
    where(stem_x_offset.c.id == others_beamed_with_me.c.id).\
    cte(name="stem_x_positions")

    self.register_stmt(stem_x_positions)

    left_right_x = select([
      func.min(stem_x_positions.c.val).label('left'),
      func.max(stem_x_positions.c.val).label('right'),
    ]).\
      cte(name="left_right_x__")

    left_right_x = select([
      left_right_x.c.left.label('left'),
      left_right_x.c.right.label('right'),
    ]).\
      where(left_right_x.c.left != None).\
      cte(name="left_right_x")

    self.register_stmt(left_right_x)

    if x_pos :
      beam_x_position_for_beams = select([
        beam.c.val.label('id'),
        left_right_x.c.left.label('left'),
        left_right_x.c.right.label('right')
      ]).\
        where(safe_eq_comp(beam.c.id, id)).\
      cte(name="beam_x_position_for_beams")

      self.register_stmt(beam_x_position_for_beams)

      self.insert = simple_insert(beam_position, beam_x_position_for_beams)
    
    else :
      #ugggghhhhhh
      end_with_staff_position = select([
        others_beamed_with_me.c.id.label('id'),
        (natural_stem_end.c.val + staff_position.c.val).label('val')
      ]).where(others_beamed_with_me.c.id == natural_stem_end.c.id).\
          where(others_beamed_with_me.c.id == staff_position.c.id).\
          cte(name = 'end_with_staff_position')

      self.register_stmt(end_with_staff_position)

      end_with_staff_position_left = end_with_staff_position.alias(name='end_with_staff_position_left')
      end_with_staff_position_right = end_with_staff_position.alias(name='end_with_staff_position_right')

      stem_x_positions_left = stem_x_positions.alias(name='stem_x_positions_left')
      stem_x_positions_right = stem_x_positions.alias(name='stem_x_positions_right')

      left_right_y = select([
        #beam.c.val.label('id'),
        end_with_staff_position_left.c.val.label('left'),
        end_with_staff_position_right.c.val.label('right')
      ]).\
        where(end_with_staff_position_left.c.id == stem_x_positions_left.c.id).\
        where(stem_x_positions_left.c.val == left_right_x.c.left).\
        where(end_with_staff_position_right.c.id == stem_x_positions_right.c.id).\
        where(stem_x_positions_right.c.val == left_right_x.c.right).\
      cte(name="left_right_y")

      self.register_stmt(left_right_y)

      slope = select([
        ((left_right_y.c.right - left_right_y.c.left) / (left_right_x.c.right - left_right_x.c.left)).label('slope')
      ]).cte(name="slope")

      self.register_stmt(slope)

      slope_offset = select([
        slope.c.slope.label('slope'),
        (left_right_y.c.left - (slope.c.slope * left_right_x.c.left)).label('offset')
      ]).cte(name="slope_offset")

      self.register_stmt(slope_offset)

      end_according_to_slope_offset = select([
        stem_x_positions.c.id.label('id'),
        ((stem_x_positions.c.val * slope_offset.c.slope) + slope_offset.c.offset).label('val')
      ]).cte(name = 'end_according_to_slope_offset')

      self.register_stmt(end_according_to_slope_offset)

      gap_with_end = select([
        end_according_to_slope_offset.c.id.label('id'),
        ((end_according_to_slope_offset.c.val - end_with_staff_position.c.val) * stem_direction.c.val).label('val')
      ]).where(stem_direction.c.id == end_with_staff_position.c.id).\
         where(end_with_staff_position.c.id == end_according_to_slope_offset.c.id).\
            cte(name = 'gap_with_end')

      self.register_stmt(gap_with_end)

      biggest_acceptable_gap_candidates = select([
        func.min(gap_with_end.c.val + 1.5).label('gap')
      ]).union(select([literal(0.0).label('gap')])).cte(name = 'biggest_acceptable_gap_candidates')

      self.register_stmt(biggest_acceptable_gap_candidates)

      biggest_acceptable_gap = select([
        func.min(biggest_acceptable_gap_candidates.c.gap).label('gap')
      ]).cte(name = 'biggest_acceptable_gap')

      self.register_stmt(biggest_acceptable_gap)

      beam_y_position_for_beams = select([
        beam.c.val.label('id'),
        (left_right_y.c.left + (biggest_acceptable_gap.c.gap * -1 * stem_direction.c.val)).label('left'),
        (left_right_y.c.right + (biggest_acceptable_gap.c.gap * -1 * stem_direction.c.val)).label('right'),
      ]).where(safe_eq_comp(beam.c.id, id)).\
        where(stem_direction.c.id == beam.c.id).\
        where(biggest_acceptable_gap.c.gap != None).\
        cte('beam_y_position_for_beams')

      self.register_stmt(beam_y_position_for_beams)
      #real_stems = select([
      #]).where()
      
      self.insert = simple_insert(beam_position, beam_y_position_for_beams)

def generate_ddl(stem_direction, natural_stem_end, staff_position, beam, x_position, stem_x_offset, beam_position, x_pos) :
  OUT = []

  insert_stmt = _Insert(stem_direction, natural_stem_end, staff_position, beam, x_position, stem_x_offset, beam_position, x_pos)
  del_stmt = _Delete(beam, beam_position)

  '''
  # test...
  when = 'WHEN (EXISTS(SELECT beam.id FROM beam {0} WHERE stem_direction.id = @ID@))'
  #only need one table, since beam is the determinant
  joins = ' '.join(['JOIN {0} ON {0}.id = beam.id'.format(tb) for tb in 'stem_direction'.split(' ')])
  when = when.format(joins)
  print when
  '''
  when = EasyWhen(beam, stem_direction, natural_stem_end, x_position, stem_x_offset, staff_position)
  #when = None
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [stem_direction, natural_stem_end, beam, x_position, stem_x_offset, staff_position]]
     #for table in [natural_stem_end, beam, x_position, ]]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = True
  #MANUAL_DDL = True
  MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager([])
  manager.ddls += generate_ddl(stem_direction = Stem_direction,
                                     natural_stem_end = Natural_stem_end,
                                     staff_position = Staff_position,
                                     beam = Beam,
                                     x_position = X_position,
                                     stem_x_offset = Stem_x_offset,
                                     beam_position = Beam_x_position,
                                     x_pos = True)

  manager.ddls += generate_ddl(stem_direction = Stem_direction,
                               natural_stem_end = Natural_stem_end,
                                     staff_position = Staff_position,
                                     beam = Beam,
                                     x_position = X_position,
                                     stem_x_offset = Stem_x_offset,
                                     beam_position = Beam_y_position,
                                     x_pos = False)

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  PLACES = [-1.0,4.0,3.0,-2.0,
            -2.0,3.0,4.0,3.0,4.0,-1.0]

  for x in range(10) :
    stmts.append((Stem_direction, {'id':x, 'val': 1}))
    stmts.append((Natural_stem_end, {'id':x, 'val': 3.0}))
    stmts.append((Staff_position, {'id':x, 'val': PLACES[x]}))
    stmts.append((X_position, {'id':x, 'val':x * 1.0}))
    stmts.append((Stem_x_offset, {'id':x, 'val':0.2}))
    stmts.append((Beam, {'id':x, 'val': 10 if x < 4 else 11}))
    
  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  print "*****&&&&&^^^%%%%"

  NOW = time.time()
  for row in conn.execute(select([Beam_y_position])).fetchall() :
    print row
  
  #for row in conn.execute(select([Beam_x_position])).fetchall() :
  #  print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
