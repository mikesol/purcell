from sqlalchemy.sql.expression import literal, distinct, exists, text, case, cast
from plain import *
import time
import emmentaler_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, stencil) :
    def where_clause_fn(id) :
      stencil.c.id == id
    DeleteStmt.__init__(self, stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, time_signature, string_box, width, height, stencil) :
    InsertStmt.__init__(self)

    string_box_a_1 = string_box.alias(name='string_box_a_1')
    string_box_a_2 = string_box.alias(name='string_box_a_2')

    time_signatures_to_xy_info = select([
      name.c.id.label('id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      cast(time_signature.c.num, String).label('num_str'),
      cast(time_signature.c.den, String).label('den_str'),
      ((width.c.val - (from_ft_20_6(string_box_a_1.c.width) * font_size.c.val / 20.0)) / 2.0).label('num_x'),
      ((width.c.val - (from_ft_20_6(string_box_a_2.c.width) * font_size.c.val / 20.0)) / 2.0).label('den_x'),
      (height.c.val - (from_ft_20_6(string_box_a_1.c.height) * font_size.c.val / 20.0)).label('num_y'),
      literal(0.0).label('den_y')
    ]).where(and_(name.c.val == 'time_signature',
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == time_signature.c.id,
                  name.c.id == width.c.id,
                  name.c.id == height.c.id,
                  font_name.c.val == string_box_a_1.c.name,
                  font_name.c.val == string_box_a_2.c.name,
                  cast(time_signature.c.num, String) == string_box_a_1.c.str,
                  cast(time_signature.c.den, String) == string_box_a_2.c.str)).\
    cte(name='time_signatures_to_xy_info')

    self.register_stmt(time_signatures_to_xy_info)

    time_signatures_to_xy_info_num = time_signatures_to_xy_info.alias('time_signatures_to_xy_info_num')
    time_signatures_to_xy_info_den = time_signatures_to_xy_info.alias('time_signatures_to_xy_info_den')
    
    time_signatures_to_stencils = select([
       time_signatures_to_xy_info.c.id.label('id'),
       literal(0).label('sub_id'),
       time_signatures_to_xy_info_num.c.font_name.label('font_name'),
       time_signatures_to_xy_info_num.c.font_size.label('font_size'),
       time_signatures_to_xy_info_num.c.num_str.label('str'),
       time_signatures_to_xy_info_num.c.num_x.label('x'),
       time_signatures_to_xy_info_num.c.num_y.label('y'),
     ]).\
       union_all(select([
       time_signatures_to_xy_info_den.c.id.label('id'),
       literal(1).label('sub_id'),
       time_signatures_to_xy_info_den.c.font_name.label('font_name'),
       time_signatures_to_xy_info_den.c.font_size.label('font_size'),
       time_signatures_to_xy_info_den.c.den_str.label('str'),
       time_signatures_to_xy_info_den.c.den_x.label('x'),
       time_signatures_to_xy_info_den.c.den_y.label('y')         
     ])).cte(name="time_signatures_to_stencils")

    self.register_stmt(time_signatures_to_stencils)

    self.insert = simple_insert(stencil, time_signatures_to_stencils)

def generate_ddl(name, font_name, font_size, time_signature, string_box, width, height, stencil) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, time_signature, string_box, width, height, stencil)

  del_stmt = _Delete(stencil)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, font_name, font_size, time_signature, width, height]]

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
                                     string_box = String_box,
                                     width = Width,
                                     height = Height,
                                     stencil = String_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  emmentaler_tools.populate_glyph_box_table(conn, Glyph_box)
  emmentaler_tools.add_to_string_box_table(conn, String_box, '3')
  emmentaler_tools.add_to_string_box_table(conn, String_box, '4')

  stmts = []

  stmts.append((Name, {'id':0,'val':'time_signature'}))
  stmts.append((Font_name, {'id':0,'val':'emmentaler-20'}))
  stmts.append((Font_size, {'id':0,'val':20}))
  stmts.append((Time_signature, {'id':0,'num':3,'den':4}))
  stmts.append((Width, {'id':0, 'val':8.40625}))
  stmts.append((Height, {'id':0, 'val':20.015625}))
  
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
