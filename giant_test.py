import tuplet_to_factor
import rhythmic_events_to_durations
import rhythmic_events_to_local_onsets
import anchor_to_referent
import local_onsets_to_global_onsets
import time

from plain import *
from properties import *
from sqlalchemy import create_engine
from sqlalchemy import event, DDL


LOG = True
ECHO = True
#ECHO = False
MANUAL_DDL = False
#MANUAL_DDL = True
#engine = create_engine('postgresql://localhost/postgres', echo=False)
engine = create_engine('sqlite:///memory', echo=ECHO)
conn = engine.connect()

generate_sqlite_functions(conn)

manager = DDL_manager()
###############################
manager.ddls += tuplet_to_factor.generate_ddl(name = Name,
                    left_tuplet_bound = Left_tuplet_bound,
                    right_tuplet_bound = Right_tuplet_bound,
                    time_next = Time_next,
                    tuplet_fraction = Tuplet_fraction,
                    tuplet_factor = Tuplet_factor)
###############################
manager.ddls += rhythmic_events_to_durations.generate_ddl(duration_log = Duration_log,
                    dots = Dots,
                    tuplet_factor = Tuplet_factor,
                    duration = Duration)
'''
###############################
manager.ddls += rhythmic_events_to_local_onsets.generate_ddl(time_next = Time_next,
                    local_onset = Local_onset,
                    duration_log = Duration_log,
                    duration = Duration)
###############################
manager.ddls += anchor_to_referent.generate_ddl(generic_referent = Onset_referent,
                             generic_next = Time_next,
                             generic_anchor = Onset_anchor)
###############################
manager.ddls += anchor_to_referent.generate_ddl(generic_referent = Horstemps_referent,
                             generic_next = Horstemps_next,
                             generic_anchor = Horstemps_anchor)
###############################
manager.ddls += local_onsets_to_global_onsets.generate_ddl(
                          global_onset = Global_onset,
                          onset_referent = Onset_referent,
                          onset_anchor = Onset_anchor,
                          local_onset = Local_onset)
'''
###############################
manager.ddls += clef_to_width.generate_ddl(name = Name,
                                   font_name = Font_name,
                                   font_size = Font_size,
                                   glyph_idx = Glyph_idx,
                                   glyph_box = Glyph_box,
                                   width = Width)

###############################
manager.ddls += time_signature_to_width.generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     time_signature = Time_signature,
                                     string_box = String_box,
                                     width = Width)

###############################
manager.ddls += accidental_to_width.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     accidental = Accidental,
                                     glyph_box = Glyph_box,
                                     accidental_width = Accidental_width)

###############################
manager.ddls += dots_to_width.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     dots = Dots,
                                     glyph_box = Glyph_box,
                                     dot_padding = Dot_padding,
                                     dot_width = Dot_width)


###############################
manager.ddls += rhythmic_events_to_right_width.generate_ddl(glyph_box = Glyph_box,
                                     note_head_width = Note_head_width,
                                     dot_width = Dot_width,
                                     rhythmic_event_to_dot_padding = Rhythmic_event_to_dot_padding,
                                     right_width = Right_width)

###############################
manager.ddls += rhythmic_events_to_left_width.generate_ddl(glyph_box = Glyph_box,
                                     note_head_width = Note_head_width,
                                     accidental_width = Accidental_width,
                                     rhythmic_event_to_accidental_padding = Rhythmic_event_to_accidental_padding,
                                     left_width = Left_width)

###############################
manager.ddls += nexts_to_graphical_next.generate_ddl(horstemps_anchor = Horstemps_anchor,
                                     horstemps_next = Horstemps_next,
                                     time_next = Time_next,
                                     graphical_next = Graphical_next)

###############################
manager.ddls += graphical_next_to_space_prev.generate_ddl(graphical_next = Graphical_next,
                                     name = Name,
                                     width = Width,
                                     left_width = Left_width,
                                     right_width = Right_width,
                                     duration = Duration,
                                     space_prev = Space_prev)

###############################
manager.ddls += space_prev_to_x_position.generate_ddl(graphical_next = Graphical_next,
                                     space_prev = Space_prev,
                                     x_position = X_position)

if not MANUAL_DDL :
  manager.register_ddls(conn, LOG = True)

Score.metadata.drop_all(engine)
Score.metadata.create_all(engine)

stmts = []

#BIG = 2**8
BIG = 2**6

#INSTR = [[0,16,2,3], [4,16,2,3], [7,19,4,5], [21,37,4,7], [37,37,2,3]]
INSTR = [[0,16,2,3], [37,37,2,3]]

for x in range(BIG) :
  if x < (BIG - 2) :
    stmts.append((Time_next, {'id':x, 'val':x + 2}))

for x in range(BIG) :
  stmts.append((Duration_log, {'id':x, 'val': (x % 4) - 2}))
  stmts.append((Dots, {'id':x, 'val':x % 3}))

for x in range(len(INSTR)) :
  stmts.append((Name, {'id':BIG + x, 'val':"tuplet"}))
  stmts.append((Left_tuplet_bound, {'id':BIG + x, 'val':INSTR[x][0]}))
  stmts.append((Right_tuplet_bound, {'id':BIG + x, 'val':INSTR[x][1]}))
  stmts.append((Tuplet_fraction, {'id':BIG + x, 'num':INSTR[x][2], 'den':INSTR[x][3]}))

BIGGER = BIG + len(INSTR)

HTS = 3

for x in range(HTS * 2) :
  stmts.append((Horstemps_next, {'id':x + BIGGER, 'val':x + BIGGER + 2}))

for x in range(2) :
  stmts.append((Horstemps_anchor, {'id':x + BIGGER, 'val':x}))

NOW = time.time()
trans = conn.begin()
for st in stmts :
  #print st
  manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  #print time.time() - NOW
trans.commit()

#for row in conn.execute(select([Onset_referent])) :
#  print row

for row in conn.execute(select([Global_onset])) :
  print row

for row in conn.execute(select([Horstemps_referent])) :
  print row

'''
print "*"*40

for row in conn.execute(select([Local_onset])) :
  print row

print "^"*40

for row in conn.execute(select([Duration])) :
  print row

print ":"*40

time.sleep(1)

conn.execute(Duration_log.update().values(val=-4).where(Duration_log.c.id==0))
#conn.execute(Duration_log.update().values(val=-4).where(Duration_log.c.id==1))
#conn.execute(Duration_log.update().values(val=-4).where(Duration_log.c.id==62))
for row in conn.execute(select([Duration])) :
  print row

for row in conn.execute(select([Local_onset])) :
  print row

print "Duration_log"+"@"*40

for row in conn.execute(select([Duration_log])) :
  print row

print "*"*40

#for row in conn.execute(select([Log_table])) :
#  print row

print "&"*40
'''