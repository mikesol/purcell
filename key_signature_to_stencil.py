from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import key_signature_tools
import emmentaler_tools
import duration_log_to_dimension

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

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, key_signature, width, key_signature_layout_info, staff_symbol, staff_space, rhythmic_event_height, glyph_stencil) :
    InsertStmt.__init__(self)

    key_signature_to_stencil_head = select([
      key_signature.c.id.label('id'),
      literal(0).label('sub_id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      case([(key_signature.c.val > 0,  21)
        ], else_ = 28).label('glyph_idx'),
      literal(0).label('x'),
      (key_signature_layout_info.c.place * staff_space.c.val * rhythmic_event_height.c.val).label('y'),
    ]).select_from(name.outerjoin(glyph_stencil, onclause=name.c.id == glyph_stencil.c.id)).\
          where(and_(key_signature_layout_info.c.accidental ==\
                    case([(key_signature.c.val > 0, 1)], else_=-1),
                  name.c.val == 'key_signature',
                  glyph_stencil.c.sub_id == None,
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == key_signature.c.id,
                  key_signature.c.val != 0)).\
         where(staff_spaceize(key_signature, staff_symbol, staff_space, rhythmic_event_height)).\
         cte(name="key_signature_to_stencil", recursive=True)

    self.register_stmt(key_signature_to_stencil_head)

    key_signature_to_stencil_prev = key_signature_to_stencil_head.\
         alias(name="key_signature_to_stencil_prev")

    key_signature_to_stencil = key_signature_to_stencil_head.union_all(
      select([
        key_signature_to_stencil_prev.c.id,
        key_signature_to_stencil_prev.c.sub_id + 1,
        key_signature_to_stencil_prev.c.font_name,
        key_signature_to_stencil_prev.c.font_size,
        key_signature_to_stencil_prev.c.glyph_idx,
        width.c.val * (key_signature_to_stencil_prev.c.sub_id + 1.0) /\
             key_signature.c.val,
        key_signature_layout_info.c.place * staff_space.c.val * rhythmic_event_height.c.val,
      ]).where(and_(key_signature_layout_info.c.accidental ==\
                    (key_signature_to_stencil_prev.c.sub_id + 2) *\
                      case([(key_signature.c.val > 0, 1)], else_=-1),
                  width.c.id == key_signature_to_stencil_prev.c.id,
                  key_signature_to_stencil_prev.c.sub_id + 1 < func.abs(key_signature.c.val),
                  key_signature_to_stencil_prev.c.id == key_signature.c.id)).\
      where(staff_spaceize(key_signature_to_stencil_prev, staff_symbol, staff_space, rhythmic_event_height)))


    self.register_stmt(key_signature_to_stencil)

    self.insert = simple_insert(glyph_stencil, key_signature_to_stencil)

def generate_ddl(name, font_name, font_size, key_signature, width, key_signature_layout_info, staff_symbol, staff_space, rhythmic_event_height, glyph_stencil) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, key_signature, width, key_signature_layout_info, staff_symbol, staff_space, rhythmic_event_height, glyph_stencil)

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
                            key_signature_layout_info = Key_signature_layout_info,
                            staff_symbol = Staff_symbol,
                            staff_space = Staff_space,
                            rhythmic_event_height = Rhythmic_event_height,
                            glyph_stencil = Glyph_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  emmentaler_tools.populate_glyph_box_table(conn, Glyph_box, from_cache=True)
  conn.execute(duration_log_to_dimension.initialize_dimensions_of_quarter_note(Glyph_box, Rhythmic_event_width, 'width'))
  conn.execute(duration_log_to_dimension.initialize_dimensions_of_quarter_note(Glyph_box, Rhythmic_event_height, 'height'))
  key_signature_tools.populate_key_signature_info_table(conn, Key_signature_layout_info)

  stmts = []

  stmts.append((Line_thickness, {'id':5, 'val':0.1}))
  stmts.append((Staff_space, {'id':5, 'val':1.0}))

  for x in [-4, -3, -2, -1, 0, 1, 2, 3,4] :
    stmts.append((Name, {'id':x,'val':'key_signature'}))
    stmts.append((Font_name, {'id':x,'val':'emmentaler-20'}))
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
