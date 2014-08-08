from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import bravura_tools
from staff_transform import staff_transform

class _Delete(DeleteStmt) :
  #def __init__(self, glyph_stencil, name) :
  def __init__(self, name, glyph_stencil) :
    def where_clause_fn(id) :
      # we NEED name to be key_signature
      # otherwise, we may delete a glyph_stencil after a staff_symbol update
      # even if the glyph is not based on staff_symbols
      # so, we localize this just to key_signatures
      stmt = select([name.c.id]).where(and_(glyph_stencil.c.id == id, name.c.id == id, name.c.val == 'key_signature'))
      return exists(stmt)
    DeleteStmt.__init__(self, glyph_stencil, where_clause_fn)

def _wind_inside_staff(v) :
  return case([(v > 2.0, v - 3.5)],else_=v)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, key_signature, width, staff_symbol, staff_space, glyph_stencil) :
    InsertStmt.__init__(self)

    key_signature_to_stencil_head = select([
      key_signature.c.id.label('id'),
      literal(0).label('sub_id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      case([(key_signature.c.val > 0,  "U+E262")
        ], else_ = "U+E260").label('unicode'),
      literal(0).label('x'),
      case([(key_signature.c.val > 0, 2.0)],else_=0.0).label('y'),
    ]).select_from(name.outerjoin(glyph_stencil, onclause=name.c.id == glyph_stencil.c.id)).\
          where(and_(
                  name.c.val == 'key_signature',
                  glyph_stencil.c.sub_id == None,
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == key_signature.c.id,
                  key_signature.c.val != 0)).\
         cte(name="key_signature_to_stencil", recursive=True)

    self.register_stmt(key_signature_to_stencil_head)

    key_signature_to_stencil_prev = key_signature_to_stencil_head.\
         alias(name="key_signature_to_stencil_prev")

    # ugh, need to fix y placement!!!
    key_signature_to_stencil = key_signature_to_stencil_head.union_all(
      select([
        key_signature_to_stencil_prev.c.id,
        key_signature_to_stencil_prev.c.sub_id + 1,
        key_signature_to_stencil_prev.c.font_name,
        key_signature_to_stencil_prev.c.font_size,
        key_signature_to_stencil_prev.c.unicode,
        width.c.val * (key_signature_to_stencil_prev.c.sub_id + 1.0) /\
             key_signature.c.val,
        _wind_inside_staff(key_signature_to_stencil_prev.c.y + case([(key_signature.c.val > 0, 2.0)],else_=1.5))
      ]).where(and_(
                  width.c.id == key_signature_to_stencil_prev.c.id,
                  key_signature_to_stencil_prev.c.sub_id + 1 < func.abs(key_signature.c.val),
                  key_signature_to_stencil_prev.c.id == key_signature.c.id)))
      


    self.register_stmt(key_signature_to_stencil)

    key_signature_to_stencil = select([
      key_signature_to_stencil.c.id.label('id'),
      key_signature_to_stencil.c.sub_id.label('sub_id'),
      key_signature_to_stencil.c.font_name.label('font_name'),
      key_signature_to_stencil.c.font_size.label('font_size'),
      key_signature_to_stencil.c.unicode.label('unicode'),
      key_signature_to_stencil.c.x.label('x'),
      # normalize from the top of the staff
      staff_transform(key_signature_to_stencil.c.y.label('y')) * staff_space.c.val
      #key_signature_to_stencil.c.y.label('y') * staff_space.c.val
      #key_signature_to_stencil.c.y.label('y'),# * staff_space.c.val
    ]).where(staff_spaceize(key_signature_to_stencil, staff_symbol, staff_space)).\
    cte(name="key_signature_to_stencil_normalized_for_staff_space")

    self.insert = simple_insert(glyph_stencil, key_signature_to_stencil)

def generate_ddl(name, font_name, font_size, key_signature, width, staff_symbol, staff_space, glyph_stencil) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, key_signature, width, staff_symbol, staff_space, glyph_stencil)

  del_stmt = _Delete(name, glyph_stencil)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, font_name, font_size, key_signature, staff_symbol, width]]

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
                            key_signature = Key_signature, 
                            width = Width,
                            staff_symbol = Staff_symbol,
                            staff_space = Staff_space,
                            glyph_stencil = Glyph_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)
  #key_signature_tools.populate_key_signature_info_table(conn, Key_signature_layout_info)

  stmts = []

  stmts.append((Line_thickness, {'id':5, 'val':0.1}))
  stmts.append((Staff_space, {'id':5, 'val':1.0}))
  #stmts.append((Key_signature_inter_accidental_padding, {'id':-1,'val':0.1}))

  for x in [-4, -3, -2, -1, 0, 1, 2, 3,4] :
    stmts.append((Name, {'id':x,'val':'key_signature'}))
    stmts.append((Font_name, {'id':x,'val':'Bravura'}))
    stmts.append((Font_size, {'id':x,'val':20}))
    stmts.append((Key_signature, {'id':x,'val': x}))
    stmts.append((Width, {'id':x,'val': x * 1.5}))
    stmts.append((Staff_symbol, {'id':x,'val': 5}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Glyph_stencil])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
