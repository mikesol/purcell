from sqlalchemy import select, exists, text, case, literal
from sqlalchemy import and_, or_
from sqlalchemy import func
from sqlalchemy import bindparam
from core_tools import *
import time

class _Delete(DeleteStmt) :
  def __init__(self, duration) :
    def where_clause_fn(id) :
      return duration.c.id == id
    DeleteStmt.__init__(self, duration, where_clause_fn)

'''
def _make_delete_statement(duration) :
  #stmt = select([duration]).where(duration.c.id == "@ID@")
  #stmt = exists(stmt).compile(compile_kwargs={"literal_binds": True})
  #print str(duration.delete().where(text(str(stmt))))
  #return duration.delete().where(text(str(stmt)))
  #return duration.delete().where(duration.c.id == "@ID@").compile(compile_kwargs={"literal_binds": True})
  ## UGGGGH
  return "DELETE FROM duration WHERE id = '@ID@'"
'''

class _Insert(InsertStmt) :
  def __init__(self, duration_log, dots, tuplet_factor,
                    duration) :

    InsertStmt.__init__(self)
    self.duration_log = duration_log
    self.dots = dots
    self.tuplet_factor = tuplet_factor
    self.duration = duration

  def _generate_stmt(self, id) :
    duration_log = self.duration_log
    dots = self.dots
    tuplet_factor = self.tuplet_factor
    duration = self.duration
    
    dot_power = select([
      duration_log.c.id.label('id'),
      literal(0).label('counter'),
      literal(1).label('times_two'),
      case([(dots.c.val != None, dots.c.val)], else_=0).label('val')
    ]).select_from(duration_log.outerjoin(dots, onclause=duration_log.c.id == dots.c.id)).\
        where(safe_eq_comp(duration_log.c.id, id)).\
        cte(name="dot_power", recursive=True)

    self.register_stmt(dot_power)
    
    dot_power_prev = dot_power.alias(name='dot_power_prev')

    dot_power = dot_power.union_all(
      select([
        dot_power_prev.c.id.label('id'),
        (dot_power_prev.c.counter + 1).label('counter'),
        (dot_power_prev.c.times_two * 2).label('times_two'),
        dot_power_prev.c.val.label('val')
      ]).\
         where(dot_power_prev.c.counter < dot_power_prev.c.val)
    )
    
    self.register_stmt(dot_power)
    
    dot_power_max = select([
      func.max(dot_power.c.counter).label('counter')
    ]).cte(name='dot_power_max')

    self.register_stmt(dot_power_max)
    
    duration_log_power = select([
      duration_log.c.id.label('id'),
      literal(0).label('counter'),
      literal(1).label('times_two'),
      func.abs(duration_log.c.val).label('val')
    ]).where(safe_eq_comp(duration_log.c.id, id)).\
        cte(name="duration_log_power", recursive=True)

    self.register_stmt(duration_log_power)
    
    duration_log_power_prev = duration_log_power.alias(name='duration_log_power_prev')

    duration_log_power = duration_log_power.union_all(
      select([
        duration_log_power_prev.c.id.label('id'),
        (duration_log_power_prev.c.counter + 1).label('counter'),
        (duration_log_power_prev.c.times_two * 2).label('times_two'),
        duration_log_power_prev.c.val.label('val')
      ]).\
         where(duration_log_power_prev.c.counter < duration_log_power_prev.c.val)
    )
    
    self.register_stmt(duration_log_power)
    
    duration_log_power_max = select([
      func.max(duration_log_power.c.counter).label('counter')
    ]).cte(name='duration_log_power_max')

    self.register_stmt(duration_log_power_max)
    
    '''
    # used to be func.pow(2, dots)
    # func.pow(2, duration_log)
    rhythmic_events_to_durations =\
      select([duration_log.c.id,
              (func.coalesce(tuplet_factor.c.num, 1) *\
              ((2 * dot_power.c.times_two) - 1 ) *\
              case([(duration_log.c.val > 0, func.pow(2,duration_log.c.val))], else_ = 1)).label('num'),
              (func.coalesce(tuplet_factor.c.den, 1) *\
              dot_power.c.times_two *\
              case([(duration_log.c.val < 0, func.pow(2,func.abs(duration_log.c.val)))], else_ = 1)).label('den')]).\
        select_from(duration_log.\
          outerjoin(tuplet_factor,
                    onclause = duration_log.c.id ==\
                       tuplet_factor.c.id)).\
        where(safe_eq_comp(duration_log.c.id, id)).\
        where(dot_power.c.counter == dot_power_max.c.counter).\
        cte(name = "rhythmic_events_to_durations")
    '''
    
    rhythmic_events_to_durations =\
      select([duration_log.c.id,
              (1.0 * (func.coalesce(tuplet_factor.c.num, 1) *\
              ((2 * dot_power.c.times_two) - 1 ) *\
              case([(duration_log.c.val > 0, duration_log_power.c.times_two)], else_ = 1))) /\
              ((func.coalesce(tuplet_factor.c.den, 1) *\
              dot_power.c.times_two *\
              case([(duration_log.c.val < 0, duration_log_power.c.times_two)], else_ = 1))).label('val')]).\
        select_from(duration_log.\
          outerjoin(tuplet_factor,
                    onclause = duration_log.c.id ==\
                       tuplet_factor.c.id)).\
        where(safe_eq_comp(duration_log.c.id, id)).\
        where(dot_power.c.counter == dot_power_max.c.counter).\
        where(duration_log_power.c.counter == duration_log_power_max.c.counter).\
        cte(name = "rhythmic_events_to_durations")

    self.register_stmt(rhythmic_events_to_durations)

    #gcdd = gcd_table(rhythmic_events_to_durations).cte(name="rhythmic_events_to_durations_gcd")

    #self.insert = duration.insert().from_select(['id','num', 'den'], gcdd)
    self.insert = simple_insert(duration, rhythmic_events_to_durations)

def generate_ddl(duration_log, dots, tuplet_factor,
                   duration, conn=None, LOG=False) :

  OUT = []

  insert_stmt = _Insert(duration_log, dots, tuplet_factor, duration)

  del_stmt = _Delete(duration)

  when = EasyWhen(duration_log)

  for table in [dots, duration_log, tuplet_factor] :  
    OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause=when)
      for action in ['INSERT', 'UPDATE', 'DELETE']]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  #ECHO = True
  MANUAL_DDL = True
  #MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(duration_log = Duration_log,
                      dots = Dots,
                      tuplet_factor = Tuplet_factor,
                      duration = Duration))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  stmts = []

  BIG = 2**5

  for x in range(BIG) :
    stmts.append((Dots, {'id':x, 'val':x % 3}))
    stmts.append((Duration_log, {'id':x, 'val': (x % 4) - 2}))
    if x < 9 :
      stmts.append((Tuplet_factor, {'id':x, 'num' : ((x % 5) + 1), 'den':5}))
  
  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  ROWS = [row for row in conn.execute(select([Duration]).order_by(Duration.c.id)).fetchall()]
  EXPECTED = [1.0 for x in range(BIG)]
  EXPECTED = [EXPECTED[x] * (2**((x % 4) - 2)) for x in range(len(EXPECTED))]
  EXPECTED = [EXPECTED[x] * (1.0 + ((2.0**(x%3) - 1.0)/(2**(x%3)))) for x in range(len(EXPECTED))]
  EXPECTED = [EXPECTED[x] * ((((x % 5) + 1.0) / 5.0) if x < 9 else 1.0) for x in range(len(EXPECTED))]

  for x in range(len(ROWS)) :
    #if not almost_equal(EXPECTED[ROWS[x][0]],(1.0 * ROWS[x][1] / ROWS[x][2])) :
    if not almost_equal(EXPECTED[ROWS[x][0]],(1.0 * ROWS[x][1])) :
      raise ValueError("Rows do not match at row {0}: {1} {2} {3} {4}".format(x, EXPECTED[ROWS[x][0]], 1.0 * ROWS[x][1] / ROWS[x][2], ROWS[x][1], ROWS[x][2] ))

  for row in conn.execute(select([Dots])).fetchall() :
    print row

  print "*"*40

  for row in conn.execute(select([Duration_log])).fetchall() :
    print row

  print "^"*40

  for row in conn.execute(select([Tuplet_factor])).fetchall() :
    print row

  print "&"*40

  for row in conn.execute(select([Duration])).fetchall() :
    print row
  '''
  manager.update(conn, Tuplet_factor, {'num':7, 'den':10}, Tuplet_factor.c.id < 10, MANUAL_DDL)
  manager.insert(conn, Tuplet_factor, {'id' : 9, 'num':7, 'den':10}, MANUAL_DDL)

  ROWS = [row for row in conn.execute(select([Duration]).order_by(Duration.c.id)).fetchall()]
  EXPECTED = [1.0 for x in range(BIG)]
  EXPECTED = [EXPECTED[x] * (2**((x % 4) - 2)) for x in range(len(EXPECTED))]
  EXPECTED = [EXPECTED[x] * (1.0 + ((2.0**(x%3) - 1.0)/(2**(x%3)))) for x in range(len(EXPECTED))]
  EXPECTED = [EXPECTED[x] * (0.7 if x < 10 else 1.0) for x in range(len(EXPECTED))]

  for x in range(len(ROWS)) :
    if not almost_equal(EXPECTED[ROWS[x][0]],(1.0 * ROWS[x][1] / ROWS[x][2])) :
      raise ValueError("Rows do not match at row {0}: {1} {2}".format(x, EXPECTED[x], 1.0 * ROWS[x][1] / ROWS[x][2] ))
  '''