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
  def __init__(self, font_name, font_size, duration_log, name, glyph_stencil) :
    InsertStmt.__init__(self)
    self.font_name = font_name
    self.font_size = font_size
    self.duration_log = duration_log
    self.name = name
    self.glyph_stencil = glyph_stencil

  def _generate_stmt(self, id) : 
    font_name = self.font_name
    font_size = self.font_size
    duration_log = self.duration_log
    name = self.name
    glyph_stencil = self.glyph_stencil

    duration_log_to_glyph_stencils = select([
      duration_log.c.id.label('id'),
      literal(0).label('sub_id'),
      font_name.c.val.label('font_name'),
      font_size.c.val.label('font_size'),
      case([(and_(duration_log.c.val == -1, name.c.val == 'note'), "U+E0A3"),
          (and_(duration_log.c.val == 0, name.c.val == 'note'), "U+E0A2"),
          (and_(duration_log.c.val == 0, name.c.val == 'rest'), "U+E4E3"),
          (and_(duration_log.c.val == -1, name.c.val == 'rest'), "U+E4E4"),
          (and_(duration_log.c.val == -2, name.c.val == 'rest'), "U+E4E5"),
          (and_(duration_log.c.val == -3, name.c.val == 'rest'), "U+E4E6"),
          (and_(duration_log.c.val == -4, name.c.val == 'rest'), "U+E4E7"),
          (and_(duration_log.c.val == -5, name.c.val == 'rest'), "U+E4E8"),
          (and_(duration_log.c.val == -6, name.c.val == 'rest'), "U+E4E9"),
          (and_(duration_log.c.val == -7, name.c.val == 'rest'), "U+E4EA"),
          (name.c.val == 'note', "U+E0A4")],
         else_ = 0).label('unicode'),
      literal(0).label('x'),
      literal(0).label('y'),
      ######
    ]).select_from(duration_log.join(font_name, onclause = duration_log.c.id == font_name.c.id).\
                   join(name, onclause = duration_log.c.id == name.c.id).\
                   join(font_size, onclause = duration_log.c.id == font_size.c.id)).\
    where(safe_eq_comp(duration_log.c.id, id)).\
    cte(name='duration_log_to_glyph_stencils')

    self.register_stmt(duration_log_to_glyph_stencils)

    self.insert = simple_insert(glyph_stencil, duration_log_to_glyph_stencils)

def generate_ddl(font_name, font_size, duration_log, name, glyph_stencil) :
  OUT = []

  insert_stmt = _Insert(font_name, font_size, duration_log, name, glyph_stencil)

  del_stmt = _Delete(glyph_stencil)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, font_size, duration_log, name]]

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
                                     duration_log = Duration_log,
                                     name = Name,
                                     glyph_stencil = Glyph_stencil))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  DL = [-4,-3,-2,-1,0]
  N = ['note', 'rest']
  for n in range(len(N)) :
    name = N[n]
    for x in range(len(DL)) :
      stmts.append((Font_name, {'id':x + (len(DL) * n),'val':'Bravura'}))
      stmts.append((Font_size, {'id':x + (len(DL) * n),'val':20}))
      stmts.append((Duration_log, {'id':x + (len(DL) * n),'val': DL[x]}))
      stmts.append((Name, {'id':x + (len(DL) * n),'val': name}))

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
