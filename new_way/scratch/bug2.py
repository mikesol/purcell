from sqlalchemy import Table, MetaData, Column, Integer, Float, String
from sqlalchemy import select
from sqlalchemy import and_, or_
from sqlalchemy import func
from plain import *

_metadata = MetaData()

def make_table(name, tp) :
  return Table(name, _metadata,
                     Column('id', Integer, primary_key = True),
                     Column('val', tp))

class JoinK(object) :
  def __init__(self, table) :
    self.table = table

class Join(JoinK) : pass
class Outerjoin(JoinK) : pass

def easy_sj(l, use_id = False, extras=[]) :
  to_join = filter(lambda x : isinstance(x, Table) or isinstance(x,JoinK), l)
  return easy_join(easy_select(l, use_id), to_join + extras)

def easy_select(l, use_id = False) :
  vals = [(t if not isinstance(t,Table) else t.c.val.label(t.name+'_val')) for t in l]
  if use_id :
    vals = [l[0].c.id.label('id')] + vals
  return select(vals)

def easy_join(stmt, lTables) :
  joins = lTables[0]
  for table in lTables[1:] :
    joink = table if isinstance(table, JoinK) else Join(table)
    joins = getattr(joins, 'join' if isinstance(joink, Join) else 'outerjoin' )(joink.table, onclause = joink.table.c.id == lTables[0].c.id)
  return stmt.select_from(joins)

def and_eq(table1, table2, vals) :
  return and_(*[table1.c[x] == table2.c[x] for x in vals])

def product(row) :
  return func.round(func.exp(func.sum(func.log(1.0 * row))))

Score = make_table('score', Integer)
Staff = make_table('staff', Integer)
Voice = make_table('voice', Integer)
Time_next = make_table('next', Integer)
Dots = make_table('dots', Integer)
Onset = make_table('onset', Float)  
Duration = make_table('duration', Float)
Duration_log = make_table('duration_log', Integer)
Name = make_table('name', String(50))
Numerator = make_table('numerator', Integer)
Denominator = make_table('denominator', Integer)
Left_bound = make_table('left_bound', Integer)
Right_bound = make_table('right_bound', Integer)

def make_statement(score, voice, name, left_bound, right_bound, duration_log, time_next, dots) :

  tuplets = easy_sj([score, voice, name, left_bound, right_bound,
                     Numerator, Denominator],
                    use_id = True).\
              where(name.c.val == "tuplet").\
              cte("tuplets")

  rhythmic_events = easy_sj([score, voice, Onset, duration_log],
                            use_id = True,
                            extras=[name]).\
                      where(or_(name.c.val == "chord",
                                name.c.val == "rest",
                                name.c.val == "space")).\
                      cte(name = "rhythmic_events")

  first_rhythmic_events = select([rhythmic_events.c.score_val,
                                  rhythmic_events.c.voice_val,
                                  func.max(rhythmic_events.c.onset_val).label('onset_val')]).\
                            group_by(rhythmic_events.c.score_val,
                                     rhythmic_events.c.voice_val).\
                            cte(name = "first_rhythmic_events")

  first_rhythmic_events_to_ids =\
       select([rhythmic_events.c.id,
               rhythmic_events.c.score_val,
               rhythmic_events.c.voice_val,
               rhythmic_events.c.onset_val]).\
         select_from(first_rhythmic_events.\
           join(rhythmic_events,
             onclause = and_eq(rhythmic_events,
                               first_rhythmic_events,
                               ['score_val', 'voice_val', 'onset_val']))).\
           cte("first_rhythmic_events_to_ids")

  #return first_rhythmic_events_to_ids.element

  rhythmic_events_to_tuplets =\
    select([rhythmic_events.c.id.label('rhythmic_id'),
            tuplets.c.id.label('tuplet_id'),
            tuplets.c.left_bound_val.label('left_val'),
            tuplets.c.right_bound_val.label('right_val')]).\
      where(and_eq(rhythmic_events, tuplets, ['score_val', 'voice_val'])).\
      cte(name = "rhythmic_events_to_tuplets", recursive = True)

  rhythmic_events_to_tuplets_a = rhythmic_events_to_tuplets.alias(name="rhythmic_events_to_tuplets_prev")

  rhythmic_events_to_tuplets =\
    rhythmic_events_to_tuplets.union_all(
      select([rhythmic_events_to_tuplets_a.c.rhythmic_id,
              rhythmic_events_to_tuplets_a.c.tuplet_id,
              time_next.c.val,
              rhythmic_events_to_tuplets_a.c.right_val]).\
      where(and_(time_next.c.val == rhythmic_events_to_tuplets_a.c.left_val,
                 time_next.c.val != rhythmic_events_to_tuplets_a.c.right_val)))


  return select([rhythmic_events_to_tuplets])

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  stmt = make_statement(score = Score,
                        voice = Voice,
                        name = Name,
                        left_bound = Left_bound,
                        right_bound = Right_bound,
                        duration_log = Duration_log,
                        time_next = Time_next,
                        dots = Dots)
  #print stmt

  engine = create_engine('sqlite:///:memory:', echo=True)
  conn = engine.connect()
  Score.metadata.create_all(engine)
  stmts = []
  for x in range(32) :
    stmts.append(Score.insert().values(id=x, val=0))
    stmts.append(Voice.insert().values(id=x, val=x%2))
    stmts.append(Duration_log.insert().values(id=x, val=-2))
  for st in stmts :
    conn.execute(st)
  #print stmt
  for row in conn.execute(stmt).fetchall() :
    print row
  