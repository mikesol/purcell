from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import bravura_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, width) :
    def where_clause_fn(id) :
      #stmt = select([name.c.id]).where(and_(width.c.id == id, name.c.id == id, name.c.val == 'key_signature'))
      #return exists(stmt)
      return width.c.id == id
    DeleteStmt.__init__(self, width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, key_signature, key_signature_inter_accidental_padding, glyph_box, width) :
    InsertStmt.__init__(self)
    self.name = name
    self.font_name = font_name
    self.font_size = font_size
    self.key_signature = key_signature
    self.key_signature_inter_accidental_padding = key_signature_inter_accidental_padding
    self.glyph_box = glyph_box
    self.width = width

  def _generate_stmt(self, id) :

    name = self.name
    font_name = self.font_name
    font_size = self.font_size
    key_signature = self.key_signature
    key_signature_inter_accidental_padding = self.key_signature_inter_accidental_padding
    glyph_box = self.glyph_box
    width = self.width

    key_signature_inter_accidental_padding_default = key_signature_inter_accidental_padding.alias(name="key_signature_inter_accidental_padding_default")
    key_signatures_to_widths = select([
      name.c.id.label('id'),
      (( ( glyph_box.c.width *\
          func.abs(key_signature.c.val)
        ) +\
         case([(func.abs(key_signature.c.val) > 0, (func.abs(key_signature.c.val) - 1) * case([(key_signature_inter_accidental_padding.c.val != None, key_signature_inter_accidental_padding.c.val)], else_ = key_signature_inter_accidental_padding_default.c.val))], else_=0.0)
       ) * font_size.c.val \
          / 20.0).label('val')
    ]).select_from(name.outerjoin(key_signature_inter_accidental_padding, onclause = name.c.id == key_signature_inter_accidental_padding.c.id)).\
          where(and_(name.c.val == 'key_signature',
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == key_signature.c.id,
                  font_name.c.val == glyph_box.c.name,
                  key_signature_inter_accidental_padding_default.c.id == -1,
                  case([(key_signature.c.val > 0,  glyph_box.c.unicode == "U+E262")], else_ = glyph_box.c.unicode == "U+E260")
                  )).\
        where(safe_eq_comp(name.c.id, id)).\
    cte(name='key_signatures_to_widths')

    self.register_stmt(key_signatures_to_widths)

    self.insert = simple_insert(width, key_signatures_to_widths)

def generate_ddl(name, font_name, font_size, key_signature, key_signature_inter_accidental_padding, glyph_box, width) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, key_signature, key_signature_inter_accidental_padding, glyph_box, width)

  #del_stmt = _Delete(width, name)
  del_stmt = _Delete(width)

  when = EasyWhen(font_name,name, key_signature)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, key_signature, key_signature_inter_accidental_padding, name]]

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
                                     key_signature_inter_accidental_padding = Key_signature_inter_accidental_padding,
                                     glyph_box = Glyph_box,
                                     width = Width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []
  stmts.append((Key_signature_inter_accidental_padding, {'id':-1,'val':0.1}))

  for x in [-3, -2, -1, 0, 1, 2, 3] :
    stmts.append((Name, {'id':x,'val':'key_signature'}))
    stmts.append((Font_name, {'id':x,'val':'Bravura'}))
    stmts.append((Font_size, {'id':x,'val':20}))
    stmts.append((Key_signature, {'id':x,'val': x}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Width])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
