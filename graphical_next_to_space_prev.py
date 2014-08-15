# for now, deals with one line of music...

from sqlalchemy.sql.expression import literal, distinct, exists, text, case, and_, or_
from plain import *
import time
import sys
import itertools

_ALL_NAMES = ['TS', 'KEY', 'CLEF', 'NOTE']

class _Delete(DeleteStmt) :
  def __init__(self, space_prev) :
    def where_clause_fn(id) :
      return or_(space_prev.c.id == id, space_prev.c.prev == id)
      #return space_prev.c.id != 3.1416
    DeleteStmt.__init__(self, space_prev, where_clause_fn)

def _is_rhythmic_event(name) :
  return or_(name.c.val == 'note', name.c.val == 'rest')

# for now this uses m4g!k values...
# also uses extra things
def _TS_NOTE(name_left, name_right, width_left, left_width) :
  val = sql_min_max([4.0, left_width.c.val + 1.0], True)
  return (and_(name_left.c.val == 'time_signature', _is_rhythmic_event(name_right)), val + width_left.c.val)

def _NOTE_TS(name_left, name_right, right_width, width_right) :
  val = sql_min_max([2.0, right_width.c.val + 1.0], True)
  return (and_(_is_rhythmic_event(name_left), name_right.c.val == 'time_signature'), val + right_width.c.val)

def _TS_KEY(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'time_signature', name_right.c.val == 'key_signature'), 2.0 + width_left.c.val)
def _KEY_TS(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'key_signature', name_right.c.val == 'time_signature'), 2.0 + width_left.c.val)

def _TS_CLEF(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'time_signature', name_right.c.val == 'clef'), 2.0 + width_left.c.val)
def _CLEF_TS(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'clef', name_right.c.val == 'time_signature'), 2.0 + width_left.c.val)

def _TS_BAR(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'time_signature', name_right.c.val == 'bar_line'), 2.0 + width_left.c.val)
def _BAR_TS(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'bar_line', name_right.c.val == 'time_signature'), 2.0 + width_left.c.val)

def _CLEF_NOTE(name_left, name_right, width_left, left_width) :
  val = sql_min_max([4.0, left_width.c.val + 1.0], True)
  return (and_(name_left.c.val == 'clef', _is_rhythmic_event(name_right)), val + width_left.c.val)
def _NOTE_CLEF(name_left, name_right, right_width, width_right) :
  val = sql_min_max([4.0, right_width.c.val + 1.0], True)
  return (and_(_is_rhythmic_event(name_left), name_right.c.val == 'clef'), val + right_width.c.val)

def _CLEF_KEY(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'clef', name_right.c.val == 'key_signature'), 2.0 + width_left.c.val)
def _KEY_CLEF(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'key_signature', name_right.c.val == 'clef'), 2.0 + width_left.c.val)

def _CLEF_BAR(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'clef', name_right.c.val == 'bar_line'), 2.0 + width_left.c.val)
def _BAR_CLEF(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'bar_line', name_right.c.val == 'clef'), 2.0 + width_left.c.val)

def _KEY_NOTE(name_left, name_right, width_left, left_width) :
  val = sql_min_max([4.0, left_width.c.val + 1.0], True)
  return (and_(name_left.c.val == 'key_signature', _is_rhythmic_event(name_right)), val + width_left.c.val)
def _NOTE_KEY(name_left, name_right, right_width, width_right) :
  val = sql_min_max([4.0, right_width.c.val + 1.0], True)
  return (and_(_is_rhythmic_event(name_left), name_right.c.val == 'key_signature'), val + right_width.c.val)

def _KEY_BAR(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'key_signature', name_right.c.val == 'bar_line'), 2.0 + width_left.c.val)
def _BAR_KEY(name_left, name_right, width_left, width_right) :
  return (and_(name_left.c.val == 'bar_line', name_right.c.val == 'key_signature'), 2.0 + width_left.c.val)

def _BAR_NOTE(name_left, name_right, width_left, left_width) :
  val = sql_min_max([4.0, left_width.c.val + 1.0], True)
  return (and_(name_left.c.val == 'bar_line', _is_rhythmic_event(name_right)), val + width_left.c.val)
def _NOTE_BAR(name_left, name_right, right_width, width_right) :
  val = sql_min_max([4.0, right_width.c.val + 1.0], True)
  return (and_(_is_rhythmic_event(name_left), name_right.c.val == 'bar_line'), val + right_width.c.val)

# for now...
def _base_space(duration) :
  #return case([(duration.c.num * 1.0 / duration.c.den > 0.25, 10.0), (duration.c.num * 1.0 / duration.c.den > 0.125, duration.c.num * 40.0 / duration.c.den)], else_ = 5.0)
  return case([(duration.c.val > 0.24, 10.0), (duration.c.val > 0.124, duration.c.val)], else_ = 5.0)

def _NOTE_NOTE(name_left, name_right, duration, right_width, left_width) :
  base_space = _base_space(duration)
  val = sql_min_max([base_space, left_width.c.val + 2.0], True)
  return (and_(_is_rhythmic_event(name_left), _is_rhythmic_event(name_right)), val + right_width.c.val)

class _Insert(InsertStmt) :
  def __init__(self, graphical_next, name, width, left_width, right_width, duration, space_prev) :
    InsertStmt.__init__(self)
    self.graphical_next = graphical_next
    self.name = name
    self.width = width
    self.left_width = left_width
    self.right_width = right_width
    self.duration = duration
    self.space_prev = space_prev

  def _generate_stmt(self, id) :
    graphical_next = self.graphical_next
    name = self.name
    width = self.width
    left_width = self.left_width
    right_width = self.right_width
    duration = self.duration
    space_prev = self.space_prev

    name_left = name.alias('name_left')
    name_right = name.alias('name_right')
    width_left = width.alias('width_left')
    width_right = width.alias('width_right')

    all_space_prev = select([
        graphical_next.c.id.label('id'),
        graphical_next.c.prev.label('prev'),
        case([
               _TS_NOTE(name_left, name_right, width_left, left_width),
               _NOTE_TS(name_left, name_right, right_width, width_right),
               _TS_KEY(name_left, name_right, width_left, width_right),
               _KEY_TS(name_left, name_right, width_left, width_right),
               _TS_CLEF(name_left, name_right, width_left, width_right),
               _CLEF_TS(name_left, name_right, width_left, width_right),
               _TS_BAR(name_left, name_right, width_left, width_right),
               _BAR_TS(name_left, name_right, width_left, width_right),

               _CLEF_NOTE(name_left, name_right, width_left, left_width),
               _NOTE_CLEF(name_left, name_right, right_width, width_right),
               _CLEF_KEY(name_left, name_right, width_left, width_right),
               _KEY_CLEF(name_left, name_right, width_left, width_right),
               _CLEF_BAR(name_left, name_right, width_left, width_right),
               _BAR_CLEF(name_left, name_right, width_left, width_right),

               _KEY_BAR(name_left, name_right, width_left, width_right),
               _BAR_KEY(name_left, name_right, width_left, width_right),
               _KEY_NOTE(name_left, name_right, width_left, left_width),
               _NOTE_KEY(name_left, name_right, right_width, width_right),

               _BAR_NOTE(name_left, name_right, width_left, left_width),
               _NOTE_BAR(name_left, name_right, right_width, width_right),

               _NOTE_NOTE(name_left, name_right, duration, right_width, left_width),
        ], else_ = 0.0).label('val'),
        #name_left.c.val,
        #name_right.c.val,
        #width_left.c.val,
        #width_right.c.val,
        #left_width.c.val,
        #right_width.c.val,
        #duration.c.num,
        #duration.c.den,
      ]).select_from(graphical_next.\
                       outerjoin(name_left, onclause = name_left.c.id == graphical_next.c.prev).\
                       outerjoin(name_right, onclause = name_right.c.id == graphical_next.c.id).\
                       outerjoin(width_left, onclause = width_left.c.id == graphical_next.c.prev).\
                       outerjoin(width_right, onclause = width_right.c.id == graphical_next.c.id).\
                       outerjoin(right_width, onclause = right_width.c.id == graphical_next.c.prev).\
                       outerjoin(left_width, onclause = left_width.c.id == graphical_next.c.id).\
                       outerjoin(duration, onclause = duration.c.id == graphical_next.c.prev)
                       ).\
      where(or_(graphical_next.c.id == id, graphical_next.c.prev == id)).\
      cte(name="all_space_prev")

    self.register_stmt(all_space_prev)

    self.insert = simple_insert(space_prev, all_space_prev)

def generate_ddl(graphical_next, name, width, left_width, right_width, duration, space_prev) :

  OUT = []

  insert_stmt = _Insert(graphical_next, name, width, left_width, right_width, duration, space_prev)

  del_stmt = _Delete(space_prev)

  # arg...dunno if graphical next is appropraite here
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [width, name, left_width, right_width, duration, graphical_next]]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  import random
  # important to get consistent results
  ######
  random.seed(0)
  ######

  ECHO = False
  #MANUAL_DDL = True
  MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(graphical_next = Graphical_next,
                                     name = Name,
                                     width = Width,
                                     left_width = Left_width,
                                     right_width = Right_width,
                                     duration = Duration,
                                     space_prev = Space_prev))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)


  stmts = []

  EXP = 5
  BIG = 2**EXP

  NAME = ['time_signature', 'key_signature','clef','bar_line'] + (['note','rest']*3)
  #NAME = ['time_signature', 'key_signature']
  DURS = [{'num':1,"den":8},{'num':1,"den":4},{'num':3,"den":8},{'num':1,"den":2},{'num':3,"den":4},{'num':1,"den":1}]

  prev_name = 0

  for x in range(BIG) :
    name = random.choice(NAME)
    if (name == prev_name) & (name in ['key_signature','time_signature','clef']) :
      name = 'note'
    prev_name = name
    dur = random.choice(DURS)
    if (x == 0) :
      stmts.append((Graphical_next, {'id':x, 'prev':None, 'next':x+1}))
    elif (x == (BIG - 1)) :
      stmts.append((Graphical_next, {'id':x, 'next':None, 'prev':x-1}))
    else :
      stmts.append((Graphical_next, {'id':x, 'next':x+1, 'prev' : x-1}))
    stmts.append((Name, {'id':x, 'val': name}))
    if name == 'rest' :
      stmts.append((Right_width, {'id':x, 'val': 5.0}))
      stmts.append((Duration, {'id' : x, 'num' : dur['num'], 'den' : dur['den']}))
    if name == 'note' :
      stmts.append((Right_width, {'id':x, 'val': 8.0}))
      stmts.append((Left_width, {'id':x, 'val': 8.0}))
      stmts.append((Duration, {'id' : x, 'num' : dur['num'], 'den' : dur['den']}))
    if name in ['time_signature','key_signature','clef','bar_line'] :
      stmts.append((Width, {'id':x, 'val': 8.0}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  print "&&&&&&&&&& DONE"

  NOW = time.time()
  for row in conn.execute(select([Space_prev.c.id, Space_prev.c.prev, Space_prev.c.val])).fetchall() :
    print row
