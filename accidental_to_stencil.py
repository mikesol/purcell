from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
#import bravura_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, glyph_stencil) :
    def where_clause_fn(id) :
      return and_(glyph_stencil.c.id == id, glyph_stencil.c.writer == 'accidental_to_stencil')
    DeleteStmt.__init__(self, glyph_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, font_name, font_size, accidental, accidental_width, rhythmic_event_to_accidental_padding, glyph_stencil) :
    InsertStmt.__init__(self)
    self.font_name = font_name
    self.font_size = font_size
    self.accidental = accidental
    self.accidental_width = accidental_width
    self.rhythmic_event_to_accidental_padding = rhythmic_event_to_accidental_padding
    self.glyph_stencil = glyph_stencil
  def _generate_stmt(self, id) :
    font_name = self.font_name
    font_size = self.font_size
    accidental = self.accidental
    accidental_width = self.accidental_width
    rhythmic_event_to_accidental_padding = self.rhythmic_event_to_accidental_padding
    glyph_stencil = self.glyph_stencil

    rhythmic_event_to_accidental_padding_a = rhythmic_event_to_accidental_padding.alias(name="rhythmic_event_to_accidental_padding_default")
    accidental_to_stencil = select([
      accidental.c.id.label('id'),
      literal('accidental_to_stencil').label('writer'),
      literal(0).label('sub_id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      case([(accidental.c.val == -1, "U+E260"),
            (accidental.c.val == 0, "U+E261"),
            (accidental.c.val == 1, "U+E262")]).label('unicode'),
      (-1 * (case([(rhythmic_event_to_accidental_padding.c.val != None, rhythmic_event_to_accidental_padding.c.val)], else_ = rhythmic_event_to_accidental_padding_a.c.val) + accidental_width.c.val)).label('x'),
      literal(0).label('y')
    ]).select_from(accidental.outerjoin(rhythmic_event_to_accidental_padding, onclause = rhythmic_event_to_accidental_padding.c.id == accidental.c.id)).\
    where(safe_eq_comp(accidental.c.id, id)).\
    where(accidental.c.id == font_name.c.id).\
    where(accidental.c.id == font_size.c.id).\
    where(accidental.c.id == accidental_width.c.id).\
    where(rhythmic_event_to_accidental_padding_a.c.id == -1).\
    cte(name='accidental_to_stencil')

    self.register_stmt(accidental_to_stencil)

    #uggghhhh....
    #real_accidental_to_stencil = realize(accidental_to_stencil, glyph_stencil, 'val')
    
    #self.register_stmt(real_accidental_to_stencil)
    #self.insert = simple_insert(glyph_stencil, real_accidental_to_stencil)
    self.insert = simple_insert(glyph_stencil, accidental_to_stencil)

def generate_ddl(font_name, font_size, accidental, accidental_width, rhythmic_event_to_accidental_padding, glyph_stencil) :
  OUT = []

  insert_stmt = _Insert(font_name, font_size, accidental, accidental_width, rhythmic_event_to_accidental_padding, glyph_stencil)

  del_stmt = _Delete(glyph_stencil)

  when = EasyWhen(font_name, font_size, accidental, accidental_width)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, font_size, accidental, accidental_width, rhythmic_event_to_accidental_padding]]

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
                                     accidental_width = Accidental_width, 
                                     rhythmic_event_to_accidental_padding = Rhythmic_event_to_accidental_padding,
                                     glyph_stencil = Glyph_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  #bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []
  stmts.append((Rhythmic_event_to_accidental_padding, {'id':-1,'val': 0.1}))

  DL = [1,0,-1,-1,0,1]
  W = [1.0,0.9,0.8,0.8,0.9,1.0]

  for x in range(len(DL)) :
    stmts.append((Font_name, {'id':x,'val':'Bravura'}))
    stmts.append((Font_size, {'id':x,'val':20}))
    stmts.append((Accidental, {'id':x,'val': DL[x]}))
    stmts.append((Accidental_width, {'id':x,'val': W[x]}))

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
