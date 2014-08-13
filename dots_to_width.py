from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import bravura_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, dot_width) :
    def where_clause_fn(id) :
      return dot_width.c.id == id
    DeleteStmt.__init__(self, dot_width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, font_name, font_size, dots, glyph_box, dot_padding, dot_width) :
    InsertStmt.__init__(self)
    self.font_name = font_name
    self.font_size = font_size
    self.dots = dots
    self.glyph_box = glyph_box
    self.dot_padding = dot_padding
    self.dot_width = dot_width

  def _generate_stmt(self, id) :
    font_name = self.font_name
    font_size = self.font_size
    dots = self.dots
    glyph_box = self.glyph_box
    dot_padding = self.dot_padding
    dot_width = self.dot_width

    dot_padding_default = dot_padding.alias(name='dot_padding_default')

    dots_to_dot_widths = select([
      dots.c.id.label('id'),
      case([(dots.c.val == 0, 0.0)], else_ = ((glyph_box.c.width * font_size.c.val * dots.c.val / 20.0) + 
        case([(dot_padding.c.val != None, dot_padding.c.val)] , else_ = dot_padding_default.c.val) * (dots.c.val - 1)
      )).label('val')
    ]).select_from(dots.outerjoin(dot_padding, onclause = dots.c.id == dot_padding.c.id)).\
        where(and_(dot_padding_default.c.id == -1,
                  dots.c.id == font_name.c.id,
                  dots.c.id == font_size.c.id,
                  font_name.c.val == glyph_box.c.name,
                  glyph_box.c.unicode == "U+E1E7")).\
        where(safe_eq_comp(dots.c.id, id)).\
    cte(name='dots_to_dot_widths')

    self.register_stmt(dots_to_dot_widths)

    self.insert = simple_insert(dot_width, dots_to_dot_widths)

def generate_ddl(font_name, font_size, dots, glyph_box, dot_padding, dot_width) :
  OUT = []

  insert_stmt = _Insert(font_name, font_size, dots, glyph_box, dot_padding, dot_width)

  del_stmt = _Delete(dot_width)

  when = EasyWhen(font_name, font_size, dots)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, font_size, dots, dot_padding]]

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

  manager = DDL_manager(generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     dots = Dots,
                                     glyph_box = Glyph_box,
                                     dot_padding = Dot_padding,
                                     dot_width = Dot_width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  stmts.append((Dot_padding, {'id': -1, 'val':0.1}))

  for x in [0,1,2,3] :
    stmts.append((Font_name, {'id':x,'val':'Bravura'}))
    stmts.append((Font_size, {'id':x,'val':20}))
    stmts.append((Dots, {'id':x,'val': x}))
    stmts.append((Dot_padding, {'id':x,'val': 0.1}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Dot_width])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
