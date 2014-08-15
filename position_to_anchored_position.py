# for now, deals with one line of music...
# we just delete everything for now...too complicated_otherwise

from sqlalchemy.sql.expression import literal, distinct, exists, text, case, and_, or_
from plain import *
import time
import sys
import itertools

class _Delete(DeleteStmt) :
  def __init__(self, anchor, anchored_table) :
    def where_clause_fn(id) :
      stmt = select([literal(id).label('id')]).cte(name="anchors", recursive=True)
      stmt_prev = stmt.alias(name='stmt_prev')
      stmt = stmt.union_all(
        select([
          anchor.c.id
        ]).where(anchor.c.val == stmt_prev.c.id)
      )
      return exists(select([stmt.c.id]).where(anchored_table.c.id == stmt.c.id))
    DeleteStmt.__init__(self, anchored_table, where_clause_fn)

# ugggh...for now, delete all......need to change this.....
class _Insert(InsertStmt) :
  def __init__(self, anchor, position, anchored_position) :
    InsertStmt.__init__(self)
    self.anchor = anchor
    self.position = position
    self.anchored_position = anchored_position

  def _generate_stmt(self, id) :
    anchor = self.anchor
    position = self.position
    anchored_position = self.anchored_position

    first_position_runner = select([
      position.c.id.label('id'),
      literal(0).label('counter'),
      anchored_position.c.val.label('val')
    ]).select_from(position.outerjoin(anchored_position, onclause = position.c.id == anchored_position.c.id)).\
      where(safe_eq_comp(position.c.id, id)).\
      where(anchored_position.c.val == None).\
      cte(name='first_position_runner', recursive = True)

    self.register_stmt(first_position_runner)
    first_position_runner_prev = first_position_runner.alias(name="first_position_runner_prev")

    first_position_runner = first_position_runner.union_all(select([
      anchor.c.val.label('id'),
      (first_position_runner_prev.c.counter + 1).label('counter'),
      anchored_position.c.val.label('val')
    ]).select_from(anchor.outerjoin(anchored_position, onclause = anchor.c.val == anchored_position.c.id)).\
      where(anchor.c.id == first_position_runner_prev.c.id).\
      where(anchored_position.c.val == None))

    self.register_stmt(first_position_runner)
    first_position_runner_max_counter =\
      select([func.max(first_position_runner.c.counter).label('max')]).cte(name="max_counter")
 
    self.register_stmt(first_position_runner_max_counter)

    starting_id = select([
      first_position_runner.c.id.label('id')
    ]).where(first_position_runner.c.counter == first_position_runner_max_counter.c.max).cte(name="starting_id")

    self.register_stmt(starting_id)

    local_position = select([
      position.c.id.label('id'),
      (position.c.val + case([(anchored_position.c.val != None, anchored_position.c.val)], else_=0.0)).label('val')
    ]).select_from(position.\
          outerjoin(anchor, onclause = position.c.id == anchor.c.id).\
          outerjoin(anchored_position, onclause = anchor.c.val == anchored_position.c.id)).\
            where(starting_id.c.id == position.c.id).\
      cte(name="position_recurser", recursive = True)

    self.register_stmt(local_position)

    local_position_prev = local_position.alias(name='local_position_prev')

    local_position = local_position.union_all(
     select([
       #local_position_prev.c.id,
       anchor.c.id,
       #local_position_prev.c.level + 1,
       local_position_prev.c.val + position.c.val
     ]).where(anchor.c.val == local_position_prev.c.id).\
        where(position.c.id == anchor.c.id)
     )

    self.register_stmt(local_position)
    
    '''
    cumulative_level = select([
      local_position.c.id.label('id'),
      func.max(local_position.c.level).label('level'),
    ]).group_by(local_position.c.id).cte(name = "cumulative_level")

    self.register_stmt(cumulative_level)
    
    cumulative_position = select([
      local_position.c.id.label('id'),
      local_position.c.val.label('val'),
    ]).where(cumulative_level.c.id == local_position.c.id).\
      where(cumulative_level.c.level == local_position.c.level).\
        cte(name = "cumulative_position")

    self.register_stmt(cumulative_position)    
    '''
    self.insert = simple_insert(anchored_position, local_position)

def generate_ddl(anchor, position, anchored_position) :

  OUT = []

  insert_stmt = _Insert(anchor, position, anchored_position)

  del_stmt = _Delete(anchor, anchored_position)

  ### UGGGH for graphical_next
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [anchor, position]]

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

  manager = DDL_manager(generate_ddl(anchor = Anchor_x,
                                     position = X_position,
                                     anchored_position = Anchored_x_position))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  for x in range(10) :
    #if x != 0 :
    #  stmts.append((Anchor_x, {'id':x,'val':x-1}))
    if x != 9 :
      stmts.append((Anchor_x, {'id':x,'val':x+1}))
    stmts.append((X_position, {'id':x, 'val':x*2.0}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  print "&&&&&&&&&& DONE"

  NOW = time.time()
  for row in conn.execute(select([Anchored_x_position])).fetchall() :
    print row

  print "%"*40

  for row in conn.execute(select([Anchor_x])).fetchall() :
    print row
