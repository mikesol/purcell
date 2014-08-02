from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, glyph_stencil) :
    def where_clause_fn(id) :
      return glyph_stencil.c.id == id
    DeleteStmt.__init__(self, glyph_stencil, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, name, font_name, font_size, glyph_idx, glyph_stencil) :
    InsertStmt.__init__(self)

    clefs_to_stencils = select([
      name.c.id.label('id'),
      literal(0).label('sub_id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      glyph_idx.c.val.label('glyph_idx'),
      literal(0).label('x'),
      literal(0).label('y'),
    ]).where(and_(name.c.val == 'clef',
                  name.c.id == font_name.c.id,
                  name.c.id == font_size.c.id,
                  name.c.id == glyph_idx.c.id,
                  )).\
    cte(name='clefs_to_stencils')

    self.register_stmt(clefs_to_stencils)

    self.insert = simple_insert(glyph_stencil, clefs_to_stencils)

def generate_ddl(name, font_name, font_size, glyph_idx, glyph_stencil) :
  OUT = []

  insert_stmt = _Insert(name, font_name, font_size, glyph_idx, glyph_stencil)

  del_stmt = _Delete(glyph_stencil)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, font_name, font_size, glyph_idx]]

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
                                     glyph_idx = Glyph_idx,
                                     glyph_stencil = Glyph_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  stmts.append((Name, {'id':0,'val':'clef'}))
  stmts.append((Font_name, {'id':0,'val':'emmentaler-20'}))
  stmts.append((Font_size, {'id':0,'val':20}))
  stmts.append((Glyph_idx, {'id':0,'val':116}))
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
