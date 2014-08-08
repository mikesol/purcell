from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import bravura_tools

from functools import partial

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, name, width) :
    def where_clause_fn(id) :
      stmt = select([name.c.id]).where(and_(width.c.id == id, name.c.id == id, name.c.val == 'clef'))
      return exists(stmt)
    DeleteStmt.__init__(self, width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, unicode, glyph_box, width) :
    InsertStmt.__init__(self)
    self.name = name
    self.font_name = font_name
    self.font_size = font_size
    self.unicode = unicode
    self.glyph_box = glyph_box
    self.width = width
  def _generate_stmt(self, id) :
    name = self.name
    font_name = self.font_name
    font_size = self.font_size
    unicode = self.unicode
    glyph_box = self.glyph_box
    width = self.width
  
    clefs_to_widths = select([
      name.c.id.label('id'),
      (glyph_box.c.width * font_size.c.val / 20.0).label('val')
    ]).where(and_(safe_eq_comp(name.c.id, id),
                  name.c.val == 'clef',
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == unicode.c.id,
                  font_name.c.val == glyph_box.c.name,
                  unicode.c.val == glyph_box.c.unicode)).\
    cte(name='clefs_to_widths')

    self.register_stmt(clefs_to_widths)

    #uggghhhh....
    #real_clefs_to_widths = realize(clefs_to_widths, width, 'val')
    
    #self.register_stmt(real_clefs_to_widths)
    #self.insert = simple_insert(width, real_clefs_to_widths)
    self.insert = simple_insert(width, clefs_to_widths)

def generate_ddl(name, font_name, font_size, unicode, glyph_box, width) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, unicode, glyph_box, width)

  del_stmt = _Delete(name, width)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, unicode, name]]

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
                                     unicode = Unicode,
                                     glyph_box = Glyph_box,
                                     width = Width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  stmts.append((Name, {'id':0,'val':'clef'}))
  stmts.append((Font_name, {'id':0,'val':'Bravura'}))
  stmts.append((Font_size, {'id':0,'val':20}))
  stmts.append((Unicode, {'id':0,'val':"U+E062"}))

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
