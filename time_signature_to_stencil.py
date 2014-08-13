from sqlalchemy.sql.expression import literal, distinct, exists, text, case, cast
from plain import *
import time
import bravura_tools
import conversion_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, glyph_stencil, name) :
  def __init__(self, name, glyph_stencil) :
    def where_clause_fn(id) :
      # we NEED name to be time_signature
      # otherwise, we may delete a glyph_stencil after a staff_symbol update
      # even if the glyph is not based on staff_symbols
      # so, we localize this just to time_signatures
      #stmt = select([name.c.id]).where(and_(glyph_stencil.c.id == id, name.c.id == id, name.c.val == 'time_signature'))
      #return exists(stmt)
      return and_(glyph_stencil.c.id == id, glyph_stencil.c.writer == 'time_signature_to_stencil')
    DeleteStmt.__init__(self, glyph_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, time_signature, glyph_box, width, staff_symbol, staff_space, stencil) :
    InsertStmt.__init__(self)
    self.name = name
    self.font_name = font_name
    self.font_size = font_size
    self.time_signature = time_signature
    self.glyph_box = glyph_box
    self.width = width
    self.staff_symbol = staff_symbol
    self.staff_space = staff_space
    self.stencil = stencil

  def _generate_stmt(self, id) :
    name = self.name
    font_name = self.font_name
    font_size = self.font_size
    time_signature = self.time_signature
    glyph_box = self.glyph_box
    width = self.width
    staff_symbol = self.staff_symbol
    staff_space = self.staff_space
    stencil = self.stencil

    glyph_box_a_1 = glyph_box.alias(name='glyph_box_a_1')
    glyph_box_a_2 = glyph_box.alias(name='glyph_box_a_2')

    time_signatures_to_xy_info = select([
      name.c.id.label('id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      conversion_tools.int_to_unicode(time_signature.c.num).label('num_str'),
      conversion_tools.int_to_unicode(time_signature.c.den).label('den_str'),
      ((width.c.val - (glyph_box_a_1.c.width * font_size.c.val / 20.0)) / 2.0).label('num_x'),
      ((width.c.val - (glyph_box_a_2.c.width * font_size.c.val / 20.0)) / 2.0).label('den_x'),
      #(height.c.val - (from_ft_20_6(string_box_a_1.c.height) * font_size.c.val / 20.0)).label('num_y'),
      #literal(0.0).label('den_y')
      (staff_space.c.val * 1.0).label('num_y'),
      (staff_space.c.val * 3.0).label('den_y'),
    ]).where(and_(name.c.val == 'time_signature',
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == time_signature.c.id,
                  name.c.id == width.c.id,
                  font_name.c.val == glyph_box_a_1.c.name,
                  font_name.c.val == glyph_box_a_2.c.name,
                  conversion_tools.int_to_unicode(time_signature.c.num) == glyph_box_a_1.c.unicode,
                  conversion_tools.int_to_unicode(time_signature.c.den) == glyph_box_a_2.c.unicode)).\
         where(staff_spaceize(name, staff_symbol, staff_space)).\
         where(safe_eq_comp(name.c.id, id)).\
    cte(name='time_signatures_to_xy_info')

    self.register_stmt(time_signatures_to_xy_info)

    time_signatures_to_xy_info_num = time_signatures_to_xy_info.alias('time_signatures_to_xy_info_num')
    time_signatures_to_xy_info_den = time_signatures_to_xy_info.alias('time_signatures_to_xy_info_den')
    
    time_signatures_to_stencils = select([
       time_signatures_to_xy_info.c.id.label('id'),
       literal('time_signature_to_stencil').label('writer'),
       literal(0).label('sub_id'),
       time_signatures_to_xy_info_num.c.font_name.label('font_name'),
       time_signatures_to_xy_info_num.c.font_size.label('font_size'),
       time_signatures_to_xy_info_num.c.num_str.label('unicode'),
       time_signatures_to_xy_info_num.c.num_x.label('x'),
       time_signatures_to_xy_info_num.c.num_y.label('y'),
     ]).\
       union_all(select([
       time_signatures_to_xy_info_den.c.id.label('id'),
       literal('time_signature_to_stencil'),
       literal(1).label('sub_id'),
       time_signatures_to_xy_info_den.c.font_name.label('font_name'),
       time_signatures_to_xy_info_den.c.font_size.label('font_size'),
       time_signatures_to_xy_info_den.c.den_str.label('unicode'),
       time_signatures_to_xy_info_den.c.den_x.label('x'),
       time_signatures_to_xy_info_den.c.den_y.label('y')         
     ])).cte(name="time_signatures_to_stencils")

    self.register_stmt(time_signatures_to_stencils)

    self.insert = simple_insert(stencil, time_signatures_to_stencils)

def generate_ddl(name, font_name, font_size, time_signature, glyph_box, width, staff_symbol, staff_space, stencil) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, time_signature, glyph_box, width, staff_symbol, staff_space, stencil)

  del_stmt = _Delete(name, stencil)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, font_name, font_size, time_signature, width, staff_symbol]]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  #MANUAL_DDL = True
  MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     time_signature = Time_signature,
                                     glyph_box = Glyph_box,
                                     width = Width,
                                     staff_symbol = Staff_symbol,
                                     staff_space = Staff_space,
                                     stencil = String_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  stmts.append((Line_thickness, {'id':5, 'val':0.1}))
  stmts.append((Staff_space, {'id':5, 'val':1.0}))

  stmts.append((Name, {'id':0,'val':'time_signature'}))
  stmts.append((Font_name, {'id':0,'val':'Bravura'}))
  stmts.append((Font_size, {'id':0,'val':20}))
  stmts.append((Time_signature, {'id':0,'num':3,'den':4}))
  stmts.append((Width, {'id':0, 'val':8.40625}))
  stmts.append((Staff_symbol, {'id':0,'val': 5}))
  #stmts.append((Height, {'id':0, 'val':20.015625}))
  
  # 554 is 4, 553 is 3, etc.

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([String_stencil])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
