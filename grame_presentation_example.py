import tuplet_to_factor
import rhythmic_events_to_durations

from plain import *
from properties import Duration_log, Dots, Time_next, Tuplet_fraction
from properties import Left_tuplet_bound, Right_tuplet_bound, Duration, Tuplet_factor
from sqlalchemy import create_engine
from sqlalchemy import event, DDL


LOG = True
ECHO = True
#ECHO = False
MANUAL_DDL = False
#MANUAL_DDL = True
TABLES_TO_REPORT = [Duration]

T = True
F = False

TUPLET_TO_FACTOR = T
RHYTHMIC_EVENTS_TO_DURATIONS = T

#engine = create_engine('postgresql://localhost/postgres', echo=False)
engine = create_engine('sqlite:///memory', echo=ECHO)
conn = engine.connect()

generate_sqlite_functions(conn)

manager = DDL_manager()

###############################
if TUPLET_TO_FACTOR :
  manager.ddls += tuplet_to_factor.generate_ddl(
                    left_tuplet_bound = Left_tuplet_bound,
                    right_tuplet_bound = Right_tuplet_bound,
                    time_next = Time_next,
                    tuplet_fraction = Tuplet_fraction,
                    tuplet_factor = Tuplet_factor)

###############################
if RHYTHMIC_EVENTS_TO_DURATIONS :
  manager.ddls += rhythmic_events_to_durations.generate_ddl(duration_log = Duration_log,
                    dots = Dots,
                    tuplet_factor = Tuplet_factor,
                    duration = Duration)


if not MANUAL_DDL :
  manager.register_ddls(conn, LOG = True)

Duration_log.metadata.drop_all(engine)
Duration_log.metadata.create_all(engine)

stmts = []

DURATION_LOGS = [-2,-1,0,-3,-2,-1]
DOTS = {}
TUPS = [(0,4,2,3),(2,3,4,5)]

for x in range(7) :
  stmts.append((Time_next, {'id': x, 'val':None if x==6 else x+1}))

for x in range(6) :
  stmts.append((Duration_log, {'id': x, 'val':DURATION_LOGS[x]}))
  if x in DOTS.keys() :
    stmts.append((Dots, {'id': x, 'val':DOTS[x]}))

for x in range(2) :
  stmts.append((Left_tuplet_bound, {'id': x + 6, 'val':TUPS[x][0]}))
  stmts.append((Right_tuplet_bound, {'id': x + 6, 'val':TUPS[x][1]}))
  stmts.append((Tuplet_fraction, {'id': x + 6, 'num':TUPS[x][2], 'den':TUPS[x][3]}))

# run!

trans = conn.begin()
for st in stmts :
  print "~~~~~~~~~~~~~~~~~~~~~~~", st[0].name, st[1]
  manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
trans.commit()

for table in TABLES_TO_REPORT :
  print "!+"*40
  print "reporting on", table.name
  print "$%"*40
  for row in conn.execute(select([table])).fetchall() :
    print row
