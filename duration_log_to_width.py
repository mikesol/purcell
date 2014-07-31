from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import emmentaler_tools

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  def __init__(self, rhythmic_event_width) :
    def where_clause_fn(id) :
      return rhythmic_event_width.c.id == id
    DeleteStmt.__init__(self, rhythmic_event_width, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, font_name, font_size, duration_log, glyph_box, name, rhythmic_event_width) :
    InsertStmt.__init__(self)

    duration_log_to_rhythmic_event_widths = select([
      duration_log.c.id.label('id'),
      (from_ft_20_6(glyph_box.c.width) * font_size.c.val / 20.0).label('val')
    ]).select_from(duration_log.join(font_name, onclause = duration_log.c.id == font_name.c.id).\
                   join(name, onclause = duration_log.c.id == name.c.id).\
                   join(font_size, onclause = duration_log.c.id == font_size.c.id).\
                   join(glyph_box, onclause = font_name.c.val == glyph_box.c.name)).where(
             and_(glyph_box.c.idx == case([(and_(duration_log.c.val == -1, name.c.val == 'note'), 148),
                                            (and_(duration_log.c.val == 0, name.c.val == 'note'), 147),
                                            (and_(duration_log.c.val == 0, name.c.val == 'rest'), 3),
                                            (and_(duration_log.c.val == -1, name.c.val == 'rest'), 4),
                                            (and_(duration_log.c.val == -2, name.c.val == 'rest'), 9),
                                            (and_(duration_log.c.val == -3, name.c.val == 'rest'), 11),
                                            (and_(duration_log.c.val == -4, name.c.val == 'rest'), 12),
                                            (and_(duration_log.c.val == -5, name.c.val == 'rest'), 13),
                                            (and_(duration_log.c.val == -6, name.c.val == 'rest'), 14),
                                            (and_(duration_log.c.val == -7, name.c.val == 'rest'), 15),
                                            (name.c.val == 'note', 149)],
                                           else_ = 0))).\
    cte(name='duration_log_to_rhythmic_event_widths')

    self.register_stmt(duration_log_to_rhythmic_event_widths)

    #uggghhhh....
    real_duration_log_to_rhythmic_event_widths = realize(duration_log_to_rhythmic_event_widths, rhythmic_event_width, 'val')
    
    self.register_stmt(real_duration_log_to_rhythmic_event_widths)
    self.insert = simple_insert(rhythmic_event_width, real_duration_log_to_rhythmic_event_widths)

def generate_ddl(font_name, font_size, duration_log, glyph_box, name, rhythmic_event_width) :
  OUT = []

  insert_stmt = _Insert(font_name, font_size, duration_log, glyph_box, name, rhythmic_event_width)

  del_stmt = _Delete(rhythmic_event_width)

  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [font_name, duration_log, name]]

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
                                     glyph_box = Glyph_box,
                                     name = Name,
                                     rhythmic_event_width = Rhythmic_event_width))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  emmentaler_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  DL = [-4,-3,-2,-1,0]
  N = ['note', 'rest']
  for n in range(len(N)) :
    name = N[n]
    for x in range(len(DL)) :
      stmts.append((Font_name, {'id':x + (len(DL) * n),'val':'emmentaler-20'}))
      stmts.append((Font_size, {'id':x + (len(DL) * n),'val':20}))
      stmts.append((Duration_log, {'id':x + (len(DL) * n),'val': DL[x]}))
      stmts.append((Name, {'id':x + (len(DL) * n),'val': name}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Rhythmic_event_width])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
