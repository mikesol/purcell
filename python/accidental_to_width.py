from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time
import bravura_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, accidental_width) :
    def where_clause_fn(id) :
      return accidental_width.c.id == id
    DeleteStmt.__init__(self, accidental_width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, font_name, font_size, accidental, glyph_box, accidental_width) :
    InsertStmt.__init__(self)
    self.font_name = font_name
    self.font_size = font_size
    self.accidental = accidental
    self.glyph_box = glyph_box
    self.accidental_width = accidental_width
  def _generate_stmt(self, id) :
    font_name = self.font_name
    font_size = self.font_size
    accidental = self.accidental
    glyph_box = self.glyph_box
    accidental_width = self.accidental_width

    accidental_to_accidental_widths = select([
      accidental.c.id.label('id'),
      (glyph_box.c.width * font_size.c.val / 20.0).label('val')
    ]).select_from(accidental.join(font_name, onclause = accidental.c.id == font_name.c.id).\
                   join(font_size, onclause = accidental.c.id == font_size.c.id).\
                   join(glyph_box, onclause = font_name.c.val == glyph_box.c.name)).where(
             and_(glyph_box.c.unicode == case([(accidental.c.val == -1, "U+E260"),
                                           (accidental.c.val == 0, "U+E261"),
                                           (accidental.c.val == 1, "U+E262")],
                                           ))).\
    where(safe_eq_comp(accidental.c.id, id)).\
    cte(name='accidental_to_accidental_widths')

    self.register_stmt(accidental_to_accidental_widths)

    #uggghhhh....
    #real_accidental_to_accidental_widths = realize(accidental_to_accidental_widths, accidental_width, 'val')
    
    #self.register_stmt(real_accidental_to_accidental_widths)
    #self.insert = simple_insert(accidental_width, real_accidental_to_accidental_widths)
    self.insert = simple_insert(accidental_width, accidental_to_accidental_widths)

def generate_ddl(font_name, font_size, accidental, glyph_box, accidental_width) :
  OUT = []

  insert_stmt = _Insert(font_name, font_size, accidental, glyph_box, accidental_width)

  del_stmt = _Delete(accidental_width)

  when = EasyWhen(font_name, font_size, accidental)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, font_size, accidental]]

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

  manager = DDL_manager(generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     accidental = Accidental,
                                     glyph_box = Glyph_box,
                                     accidental_width = Accidental_width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  DL = [1,0,-1,-1,0,1]

  for x in range(len(DL)) :
    stmts.append((Font_name, {'id':x,'val':'Bravura'}))
    stmts.append((Font_size, {'id':x,'val':20}))
    stmts.append((Accidental, {'id':x,'val': DL[x]}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Accidental_width])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
