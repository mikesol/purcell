from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time
import bravura_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, right_width) :
    def where_clause_fn(id) :
      return right_width.c.id == id
    DeleteStmt.__init__(self, right_width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, rhythmic_head_width, dot_width, rhythmic_event_to_dot_padding, right_width) :
    InsertStmt.__init__(self)
    self.rhythmic_head_width = rhythmic_head_width
    self.dot_width = dot_width
    self.rhythmic_event_to_dot_padding = rhythmic_event_to_dot_padding
    self.right_width = right_width

  def _generate_stmt(self, id) :
    rhythmic_head_width = self.rhythmic_head_width
    dot_width = self.dot_width
    rhythmic_event_to_dot_padding = self.rhythmic_event_to_dot_padding
    right_width = self.right_width
    
    rhythmic_event_to_dot_padding_a = rhythmic_event_to_dot_padding.alias(name='rhythmic_event_to_dot_padding_alias')

    rhythmic_event_to_right_widths = select([
      rhythmic_head_width.c.id.label('id'),
      (rhythmic_head_width.c.val +\
        case([(dot_width.c.val == None, 0)], else_ = dot_width.c.val) +\
        case([(dot_width.c.val == None, 0), (rhythmic_event_to_dot_padding.c.val == None, rhythmic_event_to_dot_padding_a.c.val)], else_ = rhythmic_event_to_dot_padding.c.val)).label('val')
    ]).select_from(rhythmic_head_width.outerjoin(dot_width, onclause = rhythmic_head_width.c.id == dot_width.c.id).\
         outerjoin(rhythmic_event_to_dot_padding, onclause = rhythmic_head_width.c.id == rhythmic_event_to_dot_padding.c.id)).\
       where(rhythmic_event_to_dot_padding_a.c.id == -1).\
       where(safe_eq_comp(rhythmic_head_width.c.id, id)).\
    cte(name='rhythmic_event_to_right_widths')

    self.register_stmt(rhythmic_event_to_right_widths)

    self.insert = simple_insert(right_width, rhythmic_event_to_right_widths)

def generate_ddl(rhythmic_head_width, dot_width, rhythmic_event_to_dot_padding, right_width) :
  OUT = []

  insert_stmt = _Insert(rhythmic_head_width, dot_width, rhythmic_event_to_dot_padding, right_width)

  del_stmt = _Delete(right_width)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [rhythmic_head_width, dot_width, rhythmic_event_to_dot_padding]]

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

  manager = DDL_manager(generate_ddl(rhythmic_head_width = Rhythmic_head_width,
                                     dot_width = Dot_width,
                                     rhythmic_event_to_dot_padding = Rhythmic_event_to_dot_padding,
                                     right_width = Right_width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)
  stmts = []

  stmts.append((Rhythmic_event_to_dot_padding, {'id':-1,'val': 0.1}))

  DL = [-4,-3,-2,-1,0]
  DT = [0, 1, 2, 1, 0]
  W = [0.25, 0.3, 0.4, 1.2, 0.8]
  N = [0.6, 0.2, 0.1, 5.0, 0.3]
  for x in range(len(DL)) :
    stmts.append((Duration_log, {'id':x,'val': DL[x]}))
    stmts.append((Dots, {'id':x,'val': DT[x]}))
    stmts.append((Dot_width, {'id':x,'val': W[x]}))
    stmts.append((Rhythmic_head_width, {'id':x,'val': N[x]}))
    stmts.append((Rhythmic_event_to_dot_padding, {'id':x,'val': 0.1}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Right_width])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
