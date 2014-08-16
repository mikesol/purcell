from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from core_tools import *
import time
import sys

# old long comment...
# we want the delete statement to work every time we change onset_anchor or time_next

# for time next, the adding could add a new onset referent, in which case we'll need
# a full recalculation. otherwise, we just copy the onset referent from the list.

# for onset anchor, this only matters if id changes
# when this changes, the algorithm below makes sense - we wipe all old

# in general, we want the delete statement to be as minimally invasive as possible

# we assume that if a time next is added or updated,
# it either becomes the referent if it has something
# or takes a referent (or nothing) if it does not
# the only problem here is if we want something's referent to be the beginning
# for this to work, something needs to have an onset reference to itself
# this will be our code for something at the beginning

# del statement for time next checks if if is an onset anchor and, if yes
# deletes all onset referents from list

# we also need a delete statement for onset anchor that removes anything in the before after list

class _Delete(DeleteStmt) :
  def __init__(self, table_to_delete_on, generic_next, generic_anchor, generic_anchor_statement) :
    def where_clause_fn(id) :
      linked_list_fwd = bound_range(id,
                                generic_next,
                                name="fwd_list")
      linked_list_bwd = bound_range(id,
                                generic_next,
                                forward=False,
                                name = "bwd_list")
      linked_list = select([linked_list_bwd]).union(select([linked_list_fwd])).cte(name="combined_lists")
      stmt = select([linked_list]).select_from(linked_list).where(linked_list.c.elt == table_to_delete_on.c.id)
      # the second clause guarantees that we are only deleting when
      # we are getting a new referent
      if not generic_anchor_statement :
         stmt = stmt.where(generic_anchor.c.id == id)
      return exists(stmt)
    DeleteStmt.__init__(self, table_to_delete_on, where_clause_fn)

def _make_backwards_forwards_iterator(generic_next, generic_anchor_starters, forwards) :
  name = "affected_forwards" if forwards else "affected_backwards"

  affected_stmt = select([
    generic_anchor_starters.c.id.label('id'),
    generic_anchor_starters.c.val.label('val')
    ,
  ]).cte(recursive=True, name=name)

  affected_stmt_a = affected_stmt.alias(name=name+"_prev")
  
  USING = generic_next.c.id if not forwards else generic_next.c.val
  FROM = generic_next.c.val if not forwards else generic_next.c.id

  affected_stmt = affected_stmt.union_all(
    select([
      USING,
      affected_stmt_a.c.val]).\
          where(FROM == affected_stmt_a.c.id)
  )

  return affected_stmt


class _Insert(InsertStmt) :
  def __init__(self, generic_referent, generic_next, generic_anchor) :
    InsertStmt.__init__(self)

    all_timed_objects = select([generic_next.c.id.label('id')]).\
       union(select([generic_next.c.val])).cte(name="all_generic_nexts")

    self.register_stmt(all_timed_objects)

    # ugh...this is overselective - need to prune this down
    # to only things that need...will speed up performance
    generic_anchor_starters = select([all_timed_objects.c.id.label('id'),
                                    all_timed_objects.c.id.label('val')]).\
         select_from(all_timed_objects.join(generic_anchor,
                 onclause = all_timed_objects.c.id == generic_anchor.c.id)).\
         cte(name = "generic_anchor_starters")
    
    self.register_stmt(generic_anchor_starters)

    affected_backwards = _make_backwards_forwards_iterator(generic_next, generic_anchor_starters, False)
    affected_forwards = _make_backwards_forwards_iterator(generic_next, generic_anchor_starters, True)

    # union to prevent duplicates
    everything = select([affected_backwards.c.id.label('id'),
                         affected_backwards.c.val.label('val')]).union(
                   select([affected_forwards.c.id.label('id'),
                           affected_forwards.c.val.label('val')])).cte(name = 'everything')


    self.register_stmt(everything)
    # ugh...we just did all that work and we only want to update what needs updating
    # frustrating, but 
    everything_needed = select([everything.c.id.label('id'), everything.c.val.label('val')]).\
       select_from(everything.outerjoin(generic_referent,
            onclause = everything.c.id == generic_referent.c.id)).\
          where(generic_referent.c.val == None).cte(name = 'everything_needed')

    self.register_stmt(everything_needed)

    self.insert = generic_referent.insert().from_select(['id','val'], select([everything_needed]))

def generate_ddl(generic_referent, generic_next, generic_anchor, LOG = False) :
  OUT = []

  insert_stmt = _Insert(generic_referent, generic_next, generic_anchor)

  del_stmt_or = _Delete(generic_referent, generic_next, generic_anchor, generic_anchor_statement = True)
  del_stmt_oa = _Delete(generic_anchor, generic_next, generic_anchor, generic_anchor_statement = True)

  del_stmt_tn_insert = _Delete(generic_referent, generic_next, generic_anchor, generic_anchor_statement = False)

  OUT.append(DDL_unit(generic_anchor, 'INSERT', [del_stmt_or], [insert_stmt]))
  OUT.append(DDL_unit(generic_anchor, 'INSERT', [del_stmt_oa], [], before = True))
  OUT += [DDL_unit(generic_next, action, [del_stmt_tn_insert], [insert_stmt]) for action in ['INSERT', 'UPDATE']]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  MANUAL_DDL = False
  #MANUAL_DDL = True
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(generic_referent = Onset_referent,
                            generic_next = Time_next,
                            generic_anchor = Onset_anchor))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  '''
  for ddl in ddls :
    event.listen(ddl.table, 'after_create', DDL(ddl.instruction).\
        execute_if(dialect='sqlite'))
  '''

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  NS = 2
  NV = 4
  NN = 64

  CT = 0
  for x in range(NS) :
    for y in range(NV) :
      for z in range(NN) :
        stmts.append((Duration_log, {'id' : CT, 'val' : 0}))
        stmts.append((Duration, {'id' : CT, 'num' : 1, 'den' : 1}))
        stmts.append((Local_onset, {'id' : CT, 'num' : z, 'den' : 1}))
        '''
        if (z == 0) :
          REF = CT if y == 0 else (x * NS) + ((y - 1) * NV) + (NN / 2)
          # middle of previous voice
          stmts.append((Onset_anchor, {'id' : CT, 'val' : REF}))
        '''
        if (z == 0) and (y > 0) :
          REF = (x * NS) + ((y - 1) * NV) + (NN / 2)
          # middle of previous voice
          stmts.append((Onset_anchor, {'id' : CT, 'val' : REF}))
        if z != NN - 1 :
          stmts.append((Time_next, {'id' : CT, 'val' : CT + 1 }))
        CT += 1

  #stmt = _make_insert_statement(Onset_referent, Time_next, Onset_anchor)

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  print "***********"
  for row in conn.execute(select([Onset_referent])).fetchall() :
    print row

  #stmt = _make_delete_statement(Onset_referent, Time_next, match = 68, onset_anchor_statement = True)
  #print stmt
  #conn.execute(stmt)
  manager.insert(conn, Onset_anchor.insert().values(**{'id' : 68, 'val' : 20}), MANUAL_DDL)

  #print "***********"
  #for row in conn.execute(select([Onset_referent])).fetchall() :
  #  print row
  #conn.execute(Duration.update().values(num=100, den=1).where(Duration.c.id == 4))
  
  #print "*************"
  print time.time() - NOW
  for row in conn.execute(select([Onset_referent])).fetchall() :
    print row
