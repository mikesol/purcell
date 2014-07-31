from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import emmentaler_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, width, name) :
    def where_clause_fn(id) :
      stmt = select([name.c.id]).where(and_(width.c.id == id, name.c.id == id, name.c.val == 'key_signature'))
      return exists(stmt)
    DeleteStmt.__init__(self, width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, key_signature, glyph_box, width) :
    InsertStmt.__init__(self)

    key_signatures_to_widths = select([
      name.c.id.label('id'),
      (from_ft_20_6(glyph_box.c.width) * font_size.c.val * func.abs(key_signature.c.val) / 20.0).label('val')
    ]).where(and_(name.c.val == 'key_signature',
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == key_signature.c.id,
                  font_name.c.val == glyph_box.c.name,
                  case([(key_signature.c.val > 0,  glyph_box.c.idx == 21),
                        (key_signature.c.val < 0, glyph_box.c.idx == 28)], else_ = glyph_box.c.idx == -1)
                  )).\
    cte(name='key_signatures_to_widths')

    self.register_stmt(key_signatures_to_widths)

    #uggghhhh....
    real_key_signatures_to_widths = realize(key_signatures_to_widths, width, 'val')
    
    self.register_stmt(real_key_signatures_to_widths)
    self.insert = simple_insert(width, real_key_signatures_to_widths)

def generate_ddl(name, font_name, font_size, key_signature, glyph_box, width) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, key_signature, glyph_box, width)

  del_stmt = _Delete(width, name)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, key_signature, name]]

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
                                     glyph_box = Glyph_box,
                                     width = Width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  emmentaler_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  for x in [-3, -2, -1, 0, 1, 2, 3] :
    stmts.append((Name, {'id':x,'val':'key_signature'}))
    stmts.append((Font_name, {'id':x,'val':'emmentaler-20'}))
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
