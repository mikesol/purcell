from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, glyph_stencil, name) :
  def __init__(self, name, glyph_stencil) :
    def where_clause_fn(id) :
      # we NEED name to be clef
      # otherwise, we may delete a glyph_stencil after a staff_symbol update
      # even if the glyph is not based on staff_symbols
      # so, we localize this just to clefs
      stmt = select([name.c.id]).where(and_(glyph_stencil.c.id == id, name.c.id == id, name.c.val == 'clef'))
      return exists(stmt)
    DeleteStmt.__init__(self, glyph_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, unicode, glyph_stencil) :
    InsertStmt.__init__(self)
    self.name = name
    self.font_name = font_name
    self.font_size = font_size
    self.unicode = unicode
    self.glyph_stencil = glyph_stencil
  def _generate_stmt(self, id) :
    name = self.name
    font_name = self.font_name
    font_size = self.font_size
    unicode = self.unicode
    glyph_stencil = self.glyph_stencil
    
    clefs_to_stencils = select([
      name.c.id.label('id'),
      literal(0).label('sub_id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      unicode.c.val.label('unicode'),
      literal(0).label('x'),
      literal(0).label('y'),
    ]).where(and_(name.c.val == 'clef',
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == unicode.c.id,
                  )).\
    where(safe_eq_comp(name.c.id, id)).\
    cte(name='clefs_to_stencils')

    self.register_stmt(clefs_to_stencils)

    self.insert = simple_insert(glyph_stencil, clefs_to_stencils)

def generate_ddl(name, font_name, font_size, unicode, glyph_stencil) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, unicode, glyph_stencil)

  del_stmt = _Delete(name, glyph_stencil)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, font_name, font_size, unicode]]

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
                                     glyph_stencil = Glyph_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  stmts.append((Name, {'id':0,'val':'clef'}))
  stmts.append((Font_name, {'id':0,'val':'Bravura'}))
  stmts.append((Font_size, {'id':0,'val':20}))
  stmts.append((Unicode, {'id':0,'val':"U+E050"}))
  stmts.append((X_position, {'id':0,'val':1.2}))

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
