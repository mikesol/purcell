# maybe try to merge with right width to avoid code dup.......

from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import emmentaler_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, left_width) :
    def where_clause_fn(id) :
      return left_width.c.id == id
    DeleteStmt.__init__(self, left_width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, glyph_box, note_head_width, accidental_width, rhythmic_event_to_accidental_padding, left_width) :
    InsertStmt.__init__(self)

    rhythmic_event_to_accidental_padding_a = rhythmic_event_to_accidental_padding.alias(name='rhythmic_event_to_accidental_padding_alias')

    rhythmic_event_to_left_widths = select([
      note_head_width.c.id.label('id'),
      (note_head_width.c.val +\
        case([(accidental_width.c.val == None, 0)], else_ = accidental_width.c.val) +\
        case([(accidental_width.c.val == None, 0), (rhythmic_event_to_accidental_padding.c.val == None, rhythmic_event_to_accidental_padding_a.c.val)], else_ = rhythmic_event_to_accidental_padding.c.val)).label('val')
    ]).select_from(note_head_width.outerjoin(accidental_width, onclause = note_head_width.c.id == accidental_width.c.id)).\
       where(rhythmic_event_to_accidental_padding_a.c.id == -1).\
    cte(name='rhythmic_event_to_left_widths')

    self.register_stmt(rhythmic_event_to_left_widths)

    #uggghhhh....
    real_rhythmic_event_to_left_widths = realize(rhythmic_event_to_left_widths, left_width, 'val')
    
    self.register_stmt(real_rhythmic_event_to_left_widths)
    self.insert = simple_insert(left_width, real_rhythmic_event_to_left_widths)

def generate_ddl(glyph_box, note_head_width, accidental_width, rhythmic_event_to_accidental_padding, left_width) :
  OUT = []

  insert_stmt = _Insert(glyph_box, note_head_width, accidental_width, rhythmic_event_to_accidental_padding, left_width)

  del_stmt = _Delete(left_width)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [note_head_width, accidental_width]]

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

  manager = DDL_manager(generate_ddl(glyph_box = Glyph_box,
                                     note_head_width = Note_head_width,
                                     accidental_width = Accidental_width,
                                     rhythmic_event_to_accidental_padding = Rhythmic_event_to_accidental_padding,
                                     left_width = Left_width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  emmentaler_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  stmts.append((Rhythmic_event_to_accidental_padding, {'id':-1,'val': 0.1}))

  DL = [-4,-3,-2,-1,0]
  DT = [0, 1, 2, 1, 0]
  W = [0.25, 0.3, 0.4, 1.2, 0.8]
  N = [0.6, 0.2, 0.1, 5.0, 0.3]
  for x in range(len(DL)) :
    stmts.append((Accidental_width, {'id':x,'val': W[x]}))
    stmts.append((Note_head_width, {'id':x,'val': N[x]}))
    stmts.append((Rhythmic_event_to_accidental_padding, {'id':x,'val': 0.1}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Left_width])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
