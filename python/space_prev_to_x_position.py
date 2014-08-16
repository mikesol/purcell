# for now, deals with one line of music...
# we just delete everything for now...too complicated_otherwise

from sqlalchemy.sql.expression import literal, distinct, exists, text, case, and_, or_
from core_tools import *
import time
import sys
import itertools

_ALL_NAMES = ['TS', 'KEY', 'CLEF', 'NOTE']

class _Delete(DeleteStmt) :
  def __init__(self, space_prev, x_position) :
    def where_clause_fn(id) :      
      stmt = select([literal(id).label('id')]).cte(name="anchors", recursive=True)
      stmt_prev = stmt.alias(name='stmt_prev')
      stmt = stmt.union_all(
        select([
          space_prev.c.id
        ]).where(stmt_prev.c.id == space_prev.c.prev)
      )
      return exists(select([stmt.c.id]).where(x_position.c.id == stmt.c.id))
    DeleteStmt.__init__(self, x_position, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, space_prev, x_position) :
    InsertStmt.__init__(self)
    self.space_prev = space_prev
    self.x_position = x_position

  def _generate_stmt(self, id) :
    space_prev = self.space_prev
    x_position = self.x_position

    '''
    space_prev_heads = select([
      space_prev.c.id.label('id'),
    ]).where(space_prev.c.prev == None).\
         cte(name="space_prev_heads")

    self.register_stmt(space_prev_heads)
    '''
    first_position_runner = select([
      space_prev.c.id.label('id'),
      space_prev.c.prev.label('prev'),
      literal(0).label('counter'),
      x_position.c.val.label('val')
    ]).select_from(space_prev.outerjoin(x_position, onclause = space_prev.c.id == x_position.c.id)).\
      where(safe_eq_comp(space_prev.c.id, id)).\
      where(x_position.c.val == None).\
      cte(name='first_position_runner', recursive = True)

    self.register_stmt(first_position_runner)
    first_position_runner_prev = first_position_runner.alias(name="first_position_runner_prev")

    first_position_runner = first_position_runner.union_all(select([
      space_prev.c.id.label('id'),
      space_prev.c.prev.label('prev'),
      (first_position_runner_prev.c.counter + 1).label('counter'),
      x_position.c.val.label('val')
    ]).select_from(space_prev.outerjoin(x_position, onclause = space_prev.c.id == x_position.c.id)).\
      where(space_prev.c.id == first_position_runner_prev.c.prev).\
      where(x_position.c.val == None))

    self.register_stmt(first_position_runner)
    first_position_runner_max_counter =\
      select([func.max(first_position_runner.c.counter).label('max')]).cte(name="max_counter")
 
    self.register_stmt(first_position_runner_max_counter)

    starting_id = select([
      first_position_runner.c.id.label('id')
    ]).where(first_position_runner.c.counter == first_position_runner_max_counter.c.max).cte(name="starting_id")

    start_of_chain = select([#space_prev_heads.c.id.label('id'),
      starting_id.c.id.label('id'),
      (space_prev.c.val + case([(x_position.c.val != None, x_position.c.val)], else_=0)).label('val'),
    ]).select_from(space_prev.outerjoin(x_position, onclause = space_prev.c.prev == x_position.c.id)).\
       where(space_prev.c.id == starting_id.c.id).\
       cte(name='all_x_position',recursive=True)

    self.register_stmt(start_of_chain)

    space_prev_prev = start_of_chain.alias(name='x_position_prev')

    all_x_position = start_of_chain.union_all(
      select([
        space_prev.c.id,
        space_prev_prev.c.val + space_prev.c.val
      ]).\
         where(space_prev.c.prev == space_prev_prev.c.id))

    self.register_stmt(all_x_position)


    self.insert = simple_insert(x_position, all_x_position)

def generate_ddl(space_prev, x_position) :

  OUT = []

  insert_stmt = _Insert(space_prev, x_position)

  del_stmt = _Delete(space_prev, x_position)

  ### UGGGH for graphical_next
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [space_prev]]

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

  manager = DDL_manager(generate_ddl(space_prev = Space_prev,
                                     x_position = X_position))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  EXP = 5
  BIG = 2**EXP

  for x in range(BIG) :
    #if (x == 0) :
    #  stmts.append((Graphical_next, {'id':x, 'prev':None, 'next':x+1}))
    #elif (x == (BIG - 1)) :
    #  stmts.append((Graphical_next, {'id':x, 'prev' : x-1, 'next':None}))
    #else :
    #  stmts.append((Graphical_next, {'id':x, 'next':x+1, 'prev':x-1}))
    #stmts.append((Space_prev, {'id' : x, 'prev': x+1 if x != (BIG - 1) else None, 'val' : x * 3.0}))
    stmts.append((Space_prev, {'id' : x, 'prev': x-1 if x != 0 else None, 'val' : x * 3.0}))

  print "gn"    

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  print "&&&&&&&&&& DONE"

  NOW = time.time()
  for row in conn.execute(select([X_position])).fetchall() :
    print row
