from sqlalchemy import select, exists, text
from sqlalchemy import and_, or_
from sqlalchemy import func
from sqlalchemy import bindparam
from plain import *
import time
import sys

class _Delete(DeleteStmt) :
  def __init__(self, tuplet_factor, time_next,
                    left_tuplet_bound, right_tuplet_bound) :
    def where_clause_fn(id) :
      tuplet_bounds = easy_sj([left_tuplet_bound, right_tuplet_bound], use_id=True).\
        where(left_tuplet_bound.c.id == id).cte("tuplet_bounds")
      linked_list = bound_range(tuplet_bounds.c.left_tuplet_bound_val,
                                time_next,
                                tuplet_bounds.c.right_tuplet_bound_val)
      stmt = select([linked_list]).where(linked_list.c.elt == tuplet_factor.c.id)
      stmt = exists(stmt)
      return stmt
    DeleteStmt.__init__(self, tuplet_factor, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, left_tuplet_bound,
                    right_tuplet_bound, time_next,
                    tuplet_fraction,
                    tuplet_factor) :
    InsertStmt.__init__(self)
    self.left_tuplet_bound = left_tuplet_bound
    self.right_tuplet_bound = right_tuplet_bound
    self.time_next = time_next
    self.tuplet_fraction = tuplet_fraction
    self.tuplet_factor = tuplet_factor

  # for now, don't use id...
  def _generate_stmt(self, id) :
    left_tuplet_bound = self.left_tuplet_bound
    right_tuplet_bound = self.right_tuplet_bound
    time_next = self.time_next
    tuplet_fraction = self.tuplet_fraction
    tuplet_factor = self.tuplet_factor
    
    tuplets = easy_sj([left_tuplet_bound, right_tuplet_bound,
                       tuplet_fraction],
                      use_id = True).\
                cte("tuplets")

    self.register_stmt(tuplets)

    # ugggh... tuplet_factor is NULL not working
    rhythmic_events_to_tuplets =\
      select([tuplets.c.id.label('tuplet_id'),
              tuplets.c.left_tuplet_bound_val.label('left_val'),
              tuplets.c.right_tuplet_bound_val.label('right_val'),
              tuplets.c.tuplet_fraction_num.label('numerator'),
              tuplets.c.tuplet_fraction_den.label('denominator')]).\
        select_from(tuplets.outerjoin(tuplet_factor, onclause = tuplets.c.left_tuplet_bound_val == tuplet_factor.c.id)).\
        cte(name = "rhythmic_events_to_tuplets", recursive = True)


    rhythmic_events_to_tuplets_a = rhythmic_events_to_tuplets.\
                       alias(name="rhythmic_events_to_tuplets_prev")

    rhythmic_events_to_tuplets =\
      rhythmic_events_to_tuplets.union_all(
        select([rhythmic_events_to_tuplets_a.c.tuplet_id,
                time_next.c.val,
                rhythmic_events_to_tuplets_a.c.right_val,
                rhythmic_events_to_tuplets_a.c.numerator,
                rhythmic_events_to_tuplets_a.c.denominator
                ]).\
        where(and_(time_next.c.id == rhythmic_events_to_tuplets_a.c.left_val,
                   time_next.c.id != rhythmic_events_to_tuplets_a.c.right_val)))

    self.register_stmt(rhythmic_events_to_tuplets)

    rhythmic_events_to_matching_tuplets =\
      select([rhythmic_events_to_tuplets.c.left_val.label('id'),
              product_i(rhythmic_events_to_tuplets.c.numerator).label('num'),
              product_i(rhythmic_events_to_tuplets.c.denominator).label('den')
              ]).\
        group_by(rhythmic_events_to_tuplets.c.left_val).\
        cte(name="rhythmic_events_to_matching_tuplets")

    self.register_stmt(rhythmic_events_to_matching_tuplets)

    # ugggghhh
    rhythmic_events_to_matching_tuplets_to_update =\
      select([rhythmic_events_to_matching_tuplets.c.id,
              rhythmic_events_to_matching_tuplets.c.num,
              rhythmic_events_to_matching_tuplets.c.den
              ]).\
        select_from(rhythmic_events_to_matching_tuplets.\
             outerjoin(tuplet_factor, onclause = tuplet_factor.c.id == rhythmic_events_to_matching_tuplets.c.id)).\
        where(tuplet_factor.c.num == None).\
        cte(name="rhythmic_events_to_matching_tuplets_to_update")

    self.register_stmt(rhythmic_events_to_matching_tuplets_to_update)

    self.insert = tuplet_factor.insert().from_select(['id','num','den'],
                    rhythmic_events_to_matching_tuplets_to_update)

def generate_ddl(left_tuplet_bound,
                   right_tuplet_bound, time_next,
                   tuplet_fraction,
                   tuplet_factor) :

  OUT = []

  insert_stmt = _Insert(left_tuplet_bound, right_tuplet_bound,
                         time_next, tuplet_fraction,
                         tuplet_factor)

  del_stmt = _Delete(tuplet_factor, time_next,
                          left_tuplet_bound, right_tuplet_bound)

  #for table in [tuplet_fraction, left_tuplet_bound, right_tuplet_bound, name] :
  for table in [tuplet_fraction, left_tuplet_bound, right_tuplet_bound, time_next] :
    OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt]) for action in ['INSERT', 'UPDATE', 'DELETE']]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  
  ECHO = False
  MANUAL_DDL = True
  #MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(
                        left_tuplet_bound = Left_tuplet_bound,
                        right_tuplet_bound = Right_tuplet_bound,
                        time_next = Time_next,
                        tuplet_fraction = Tuplet_fraction,
                        tuplet_factor = Tuplet_factor))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  BIG = 2**6

  for x in range(BIG) :
    if x < (BIG - 2) :
      stmts.append((Time_next, {'id' : x, 'val' : x + 2}))

  '''
  Tests coverings, groups of 1
  '''
  INSTR = [[0,16,2,3], [4,16,2,3], [7,19,4,5], [21,37,4,7], [37,37,2,3]]

  for x in range(len(INSTR)) :
    stmts.append((Left_tuplet_bound, {'id':BIG + x, 'val':INSTR[x][0]}))
    stmts.append((Right_tuplet_bound, {'id':BIG + x, 'val':INSTR[x][1]}))
    stmts.append((Tuplet_fraction, {'id':BIG + x, 'num':INSTR[x][2], 'den':INSTR[x][3]}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  ROWS = [row for row in conn.execute(select([Tuplet_factor])).fetchall()]
  EXPECTED = [(1,1) for x in range(BIG)]
  for x in range(len(INSTR)) :
    for y in range(INSTR[x][0], INSTR[x][1] + 1) :
      if (y + INSTR[x][0]) % 2 == 0 :
        EXPECTED[y] = (EXPECTED[y][0] * INSTR[x][2], EXPECTED[y][1] * INSTR[x][3])

  #print ROWS
  #print EXPECTED

  for x in range(len(ROWS)) :
    if EXPECTED[ROWS[x][0]] != ROWS[x][1:] :
      raise ValueError("Rows do not match at row {0} {1}: {1} {2}".format(x, ROWS[x][0], EXPECTED[ROWS[x][0]], ROWS[x][1:]))

  manager.update(conn, Tuplet_fraction.update().values(**{'num':7, 'den':3}).where(Tuplet_fraction.c.id == BIG), MANUAL_DDL)
  INSTR[0][2] = 7
  
  ROWS = [row for row in conn.execute(select([Tuplet_factor])).fetchall()]
  EXPECTED = [(1,1) for x in range(BIG)]
  for x in range(len(INSTR)) :
    for y in range(INSTR[x][0], INSTR[x][1] + 1) :
      if (y + INSTR[x][0]) % 2 == 0 :
        EXPECTED[y] = (EXPECTED[y][0] * INSTR[x][2], EXPECTED[y][1] * INSTR[x][3])

  for x in range(len(ROWS)) :
    if EXPECTED[ROWS[x][0]] != ROWS[x][1:] :
      raise ValueError("Rows do not match at row {0}: {1} {2}".format(x, EXPECTED[x], ROWS[x][1:]))

  #for row in conn.execute(select([Log_table])).fetchall() :
  #  print row