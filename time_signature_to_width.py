from sqlalchemy.sql.expression import literal, distinct, exists, text, case, cast
from plain import *
import time
import bravura_tools
import conversion_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, width) :
    def where_clause_fn(id) :
      #stmt = select([name.c.id]).where(and_(width.c.id == id, name.c.id == id, name.c.val == 'time_signature'))
      #return exists(stmt)
      return width.c.val == id
    DeleteStmt.__init__(self, width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, time_signature, glyph_box, width) :
    InsertStmt.__init__(self)

    glyph_box_a_1 = glyph_box.alias(name='glyph_box_a_1')
    glyph_box_a_2 = glyph_box.alias(name='glyph_box_a_2')

    time_signatures_to_widths = select([
      name.c.id.label('id'),
      sql_min_max([(glyph_box_a_1.c.width * font_size.c.val / 20.0),
                 (glyph_box_a_2.c.width * font_size.c.val / 20.0)], True).label('val')
    ]).where(and_(name.c.val == 'time_signature',
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == time_signature.c.id,
                  font_name.c.val == glyph_box_a_1.c.name,
                  font_name.c.val == glyph_box_a_2.c.name,
                  conversion_tools.int_to_unicode(time_signature.c.num) == glyph_box_a_1.c.unicode,
                  conversion_tools.int_to_unicode(time_signature.c.den) == glyph_box_a_2.c.unicode)).\
    cte(name='time_signatures_to_widths')

    self.register_stmt(time_signatures_to_widths)

    #uggghhhh....
    real_time_signatures_to_widths = realize(time_signatures_to_widths, width, 'val')
    
    self.register_stmt(real_time_signatures_to_widths)
    self.insert = simple_insert(width, real_time_signatures_to_widths)

def generate_ddl(name, font_name, font_size, time_signature, glyph_box, width) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, time_signature, glyph_box, width)

  del_stmt = _Delete(width)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, font_name, font_size, time_signature]]

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
                                     width = Width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  stmts.append((Name, {'id':0,'val':'time_signature'}))
  stmts.append((Font_name, {'id':0,'val':'Bravura'}))
  stmts.append((Font_size, {'id':0,'val':20}))
  stmts.append((Time_signature, {'id':0,'num':3,'den':4}))
  # 554 is 4, 553 is 3, etc.

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
