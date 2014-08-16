from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time
import bravura_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, glyph_stencil, name) :
  def __init__(self, writer_name, glyph_stencil) :
    def where_clause_fn(id) :
      return and_(glyph_stencil.c.id == id, glyph_stencil.c.writer == writer_name)
    DeleteStmt.__init__(self, glyph_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, font_name, font_size, unicode, glyph_box, alignment_directive, glyph_stencil, writer, extra_eq) :
    InsertStmt.__init__(self)
    self.font_name = font_name
    self.font_size = font_size
    self.unicode = unicode
    self.glyph_box = glyph_box
    self.alignment_directive = alignment_directive
    self.glyph_stencil = glyph_stencil
    self.writer = writer
    self.extra_eq = extra_eq

  def _generate_stmt(self, id) :
    font_name = self.font_name
    font_size = self.font_size
    unicode = self.unicode
    glyph_box = self.glyph_box
    glyph_stencil = self.glyph_stencil
    alignment_directive = self.alignment_directive
    writer = self.writer
    extra_eq = self.extra_eq
    
    generics_to_stencils = select([
      font_name.c.id.label('id'),
      literal(writer).label('writer'),
      literal(0).label('sub_id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      unicode.c.val.label('unicode'),
      case([(alignment_directive.c.x != None, glyph_box.c.x + (alignment_directive.c.x * glyph_box.c.width))], else_=0).label('x'),
      case([(alignment_directive.c.y != None, glyph_box.c.y + glyph_box.c.height - (alignment_directive.c.y * glyph_box.c.height))], else_=0).label('y'),
    ]).select_from(font_name.outerjoin(alignment_directive, onclause = alignment_directive.c.id == font_name.c.id)).\
        where(safe_eq_comp(font_name.c.id, id)).\
        where(and_(glyph_box.c.name == font_name.c.val,
                  glyph_box.c.unicode == unicode.c.val,
                  font_name.c.id == font_size.c.id,
                  font_name.c.id == unicode.c.id,
                  *extra_eq
                  )).\
    cte(name='generics_to_stencils')

    self.register_stmt(generics_to_stencils)

    self.insert = simple_insert(glyph_stencil, generics_to_stencils)

def generate_ddl(font_name, font_size, unicode, glyph_box, alignment_directive, glyph_stencil, writer, extra_eq) :
  OUT = []

  insert_stmt = _Insert(font_name, font_size, unicode, glyph_box, alignment_directive, glyph_stencil, writer, extra_eq)

  del_stmt = _Delete(writer, glyph_stencil)

  when = EasyWhen(font_name, font_size, unicode)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, font_size, unicode, alignment_directive]]

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
                                     unicode = Unicode,
                                     glyph_box = Glyph_box,
                                     alignment_directive = Alignment_directive,
                                     glyph_stencil = Glyph_stencil,
                                     writer = 'clef_to_stencil',
                                     #extra_eq = [Name.c.val == 'clef', Name.c.id == Font_name.c.id]
                                     extra_eq = []
                                     ))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  for x in range(3) :
    stmts.append((Name, {'id':x,'val':'clef'}))
    stmts.append((Font_name, {'id':x,'val':'Bravura'}))
    stmts.append((Font_size, {'id':x,'val':20}))
    stmts.append((Unicode, {'id':x,'val':"U+E050"}))
    stmts.append((X_position, {'id':x,'val':1.2}))
    AD = [(None, None), (1.0, 1.0), (0.0, 0.0)]
    stmts.append((Alignment_directive, {'id':x,'x':AD[x][0], 'y':AD[x][1]}))

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
