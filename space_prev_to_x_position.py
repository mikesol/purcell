# for now, deals with one line of music...

from sqlalchemy.sql.expression import literal, distinct, exists, text, case, and_, or_
from plain import *
import time
import sys
import itertools

_ALL_NAMES = ['TS', 'KEY', 'CLEF', 'NOTE']

class _Delete(DeleteStmt) :
  def __init__(self, space_prev, x_position) :
    def where_clause_fn(id) :
      ''''
      strain = bound_range(id, space_prev)
      stmt = select([strain]).where(strain.c.elt == x_position.c.id)
      stmt = exists(stmt)
      return stmt
      '''
      return x_position.c.val != 3.1416
    DeleteStmt.__init__(self, x_position, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, graphical_next, space_prev, x_position) :
    InsertStmt.__init__(self)

    graphical_next_heads = select([
      graphical_next.c.id.label('id'),
    ]).where(graphical_next.c.prev == None).\
         cte(name="graphical_next_heads")

    self.register_stmt(graphical_next_heads)

    start_of_chain = select([graphical_next_heads.c.id.label('id'),
    space_prev.c.val.label('val'),
    ]).where(space_prev.c.id == graphical_next_heads.c.id).\
       cte(name='all_x_position',recursive=True)

    self.register_stmt(start_of_chain)

    space_prev_prev = start_of_chain.alias(name='x_position_prev')

    all_x_position = start_of_chain.union_all(
      select([
        graphical_next.c.next,
        space_prev_prev.c.val + space_prev.c.val
      ]).\
         where(and_(space_prev.c.id == graphical_next.c.next,
                    graphical_next.c.id == space_prev_prev.c.id)))

    self.register_stmt(all_x_position)

    giant_kludge = realize(all_x_position, x_position, 'val')

    self.register_stmt(giant_kludge)

    self.insert = simple_insert(x_position, giant_kludge)

def generate_ddl(graphical_next, space_prev, x_position) :

  OUT = []

  insert_stmt = _Insert(graphical_next, space_prev, x_position)

  del_stmt = _Delete(space_prev, x_position)

  ### UGGGH for graphical_next
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [space_prev, graphical_next]]

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

  manager = DDL_manager(generate_ddl(graphical_next = Graphical_next,
                                     space_prev = Space_prev,
                                     x_position = X_position))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)


  stmts = []

  EXP = 5
  BIG = 2**EXP

  for x in range(BIG) :
    if (x == 0) :
      stmts.append((Graphical_next, {'id':x, 'prev':None, 'next':x+1}))
    elif (x == (BIG - 1)) :
      stmts.append((Graphical_next, {'id':x, 'prev' : x-1, 'next':None}))
    else :
      stmts.append((Graphical_next, {'id':x, 'next':x+1, 'prev':x-1}))
    stmts.append((Space_prev, {'id' : x, 'val' : x * 3.0}))
    

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  print "&&&&&&&&&& DONE"

  NOW = time.time()
  for row in conn.execute(select([X_position])).fetchall() :
    print row
