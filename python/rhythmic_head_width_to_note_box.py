from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time
import bravura_tools
from staff_transform import staff_transform

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, rhythmic_head_width, note_box) :
    def where_clause_fn(id) :
      stmt = select([rhythmic_head_width.c.id]).where(and_(note_box.c.id == id, rhythmic_head_width.c.id == id))
      return exists(stmt)
    DeleteStmt.__init__(self, note_box, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, rhythmic_head_width, stem_end, staff_position, note_box) :
    InsertStmt.__init__(self)
    self.rhythmic_head_width = rhythmic_head_width
    self.stem_end = stem_end
    self.staff_position = staff_position
    self.note_box = note_box

  def _generate_stmt(self, id) :
    rhythmic_head_width = self.rhythmic_head_width
    stem_end = self.stem_end
    staff_position = self.staff_position
    note_box = self.note_box
    
    candidates = select([
      literal(0.0).label('x'),
      staff_position.c.val.label('y')
    ]).\
       where(safe_eq_comp(staff_position.c.id, id)).union_all(select([
      rhythmic_head_width.c.val.label('x'),
      (staff_position.c.val + stem_end.c.val).label('y')
    ]).where(safe_eq_comp(staff_position.c.id, id)).\
         where(rhythmic_head_width.c.id == staff_position.c.id).\
         where(staff_position.c.id == stem_end.c.id)).cte(name="box_candidates")

    self.register_stmt(candidates)

    rhythmic_head_width_to_note_box = select([
      rhythmic_head_width.c.id.label('id'),
      func.min(candidates.c.x).label('x'),
      func.min(candidates.c.y).label('y'),
      func.max(candidates.c.x).label('width'),
      func.max(candidates.c.y).label('height'),
    ]).where(safe_eq_comp(rhythmic_head_width.c.id, id)).\
    cte(name='rhythmic_head_width_to_note_box_pre')

    rhythmic_head_width_to_note_box = select([
      rhythmic_head_width_to_note_box.c.id.label('id'),
      rhythmic_head_width_to_note_box.c.x.label('x'),
      rhythmic_head_width_to_note_box.c.y.label('y'),
      rhythmic_head_width_to_note_box.c.width.label('width'),
      rhythmic_head_width_to_note_box.c.height.label('height'),
    ]).where(safe_eq_comp(rhythmic_head_width.c.id, id)).\
    where(rhythmic_head_width_to_note_box.c.x != None).\
    cte(name='rhythmic_head_width_to_note_box')

    self.register_stmt(rhythmic_head_width_to_note_box)

    self.insert = simple_insert(note_box, rhythmic_head_width_to_note_box)

def generate_ddl(rhythmic_head_width, stem_end, staff_position, note_box) :
  OUT = []

  insert_stmt = _Insert(rhythmic_head_width, stem_end, staff_position, note_box)

  del_stmt = _Delete(rhythmic_head_width, note_box)
  
  when = EasyWhen(rhythmic_head_width, stem_end, staff_position)
  
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [rhythmic_head_width, stem_end, staff_position]]

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

  # for now, no rests
  manager = DDL_manager(generate_ddl(rhythmic_head_width = Rhythmic_head_width,
    stem_end = Stem_end,
    staff_position = Staff_position,
    note_box = Note_box))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  for x in range(5) :
    stmts.append((Rhythmic_head_width, {'id':x, 'val':x * 0.5}))
    stmts.append((Stem_end, {'id':x, 'val':3.0 * (((x % 2) * 2) - 1)}))
    stmts.append((Staff_position, {'id':x, 'val':0.0}))

  for stmt in stmts :
    print stmt[1]

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Note_box])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
