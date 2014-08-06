import tuplet_to_factor
import rhythmic_events_to_durations
import clef_to_width
import key_signature_to_width
import time_signature_to_width
import time_signature_to_height
import accidental_to_width
import dots_to_width
import rhythmic_events_to_right_width
import rhythmic_events_to_left_width
import nexts_to_graphical_next
import graphical_next_to_space_prev
import space_prev_to_x_position
import duration_log_to_dimension

import staff_symbol_to_stencil
import clef_to_stencil
import time_signature_to_stencil
import key_signature_to_stencil
import duration_log_to_stencil

import emmentaler_tools
import key_signature_tools
import simple_http_server

from plain import *
from properties import *
from sqlalchemy import create_engine
from sqlalchemy import event, DDL, text

import json
import time
import sys

print select([Line_stencil, X_position]).select_from(Line_stencil.join(X_position, onclause = Line_stencil.c.id == X_position.c.id))

LOG = True
#ECHO = True
ECHO = False
MANUAL_DDL = False
#MANUAL_DDL = True
TABLES_TO_REPORT = [Line_stencil, Glyph_stencil, String_stencil, X_position]

T = True
F = False

TUPLET_TO_FACTOR = T
RHYTHMIC_EVENTS_TO_DURATIONS = T
CLEF_TO_WIDTH = T
KEY_SIGNATURE_TO_WIDTH = T
TIME_SIGNATURE_TO_WIDTH = T
TIME_SIGNATURE_TO_HEIGHT = T
ACCIDENTAL_TO_WIDTH = T
DOTS_TO_WIDTH = T
RHYTHMIC_EVENTS_TO_RIGHT_WIDTH = T
RHYTHMIC_EVENTS_TO_LEFT_WIDTH = T
NEXTS_TO_GRAPHICAL_NEXT  = T
GRAPHICAL_NEXT_TO_SPACE_PREV = T
SPACE_PREV_TO_X_POSITION  = T
DURATION_LOG_TO_WIDTH = T
DURATION_LOG_TO_HEIGHT = T
STAFF_SYMBOL_TO_STENCIL = T
CLEF_TO_STENCIL = T
TIME_SIGNATURE_TO_STENCIL = T
KEY_SIGNATURE_TO_STENCIL = T
DURATION_LOG_TO_STENCIL = T

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

###############################
if KEY_SIGNATURE_TO_WIDTH :
  manager.ddls += key_signature_to_width.generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     key_signature = Key_signature,
                                     glyph_box = Glyph_box,
                                     width = Width)

###############################
if CLEF_TO_WIDTH : 
  manager.ddls += clef_to_width.generate_ddl(name = Name,
                                   font_name = Font_name,
                                   font_size = Font_size,
                                   glyph_idx = Glyph_idx,
                                   glyph_box = Glyph_box,
                                   width = Width)

###############################
if TIME_SIGNATURE_TO_WIDTH : 
  manager.ddls += time_signature_to_width.generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     time_signature = Time_signature,
                                     string_box = String_box,
                                     width = Width)

###############################
if TIME_SIGNATURE_TO_HEIGHT :
  manager.ddls += time_signature_to_height.generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     time_signature = Time_signature,
                                     string_box = String_box,
                                     time_signature_inter_number_padding = Time_signature_inter_number_padding,
                                     height = Height)

###############################
if ACCIDENTAL_TO_WIDTH :
  manager.ddls += accidental_to_width.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     accidental = Accidental,
                                     glyph_box = Glyph_box,
                                     accidental_width = Accidental_width)

################################
if DURATION_LOG_TO_WIDTH :
  manager.ddls += duration_log_to_dimension.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     duration_log = Duration_log,
                                     glyph_box = Glyph_box,
                                     name = Name,
                                     rhythmic_event_dimension = Rhythmic_event_width,
                                     dimension = 'width')

################################
if DURATION_LOG_TO_HEIGHT :
  manager.ddls += duration_log_to_dimension.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     duration_log = Duration_log,
                                     glyph_box = Glyph_box,
                                     name = Name,
                                     rhythmic_event_dimension = Rhythmic_event_height,
                                     dimension = 'height')

###############################
if DOTS_TO_WIDTH :
  manager.ddls += dots_to_width.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     dots = Dots,
                                     glyph_box = Glyph_box,
                                     dot_padding = Dot_padding,
                                     dot_width = Dot_width)


###############################
if RHYTHMIC_EVENTS_TO_RIGHT_WIDTH :
  manager.ddls += rhythmic_events_to_right_width.generate_ddl(glyph_box = Glyph_box,
                                     rhythmic_event_width = Rhythmic_event_width,
                                     dot_width = Dot_width,
                                     rhythmic_event_to_dot_padding = Rhythmic_event_to_dot_padding,
                                     right_width = Right_width)

###############################
if RHYTHMIC_EVENTS_TO_LEFT_WIDTH :
  manager.ddls += rhythmic_events_to_left_width.generate_ddl(glyph_box = Glyph_box,
                                     rhythmic_event_width = Rhythmic_event_width,
                                     accidental_width = Accidental_width,
                                     rhythmic_event_to_accidental_padding = Rhythmic_event_to_accidental_padding,
                                     left_width = Left_width)

###############################
if NEXTS_TO_GRAPHICAL_NEXT :
  manager.ddls += nexts_to_graphical_next.generate_ddl(horstemps_anchor = Horstemps_anchor,
                                     horstemps_next = Horstemps_next,
                                     time_next = Time_next,
                                     graphical_next = Graphical_next)

###############################
if GRAPHICAL_NEXT_TO_SPACE_PREV :
  manager.ddls += graphical_next_to_space_prev.generate_ddl(graphical_next = Graphical_next,
                                     name = Name,
                                     width = Width,
                                     left_width = Left_width,
                                     right_width = Right_width,
                                     duration = Duration,
                                     space_prev = Space_prev)

###############################
if SPACE_PREV_TO_X_POSITION :
  manager.ddls += space_prev_to_x_position.generate_ddl(graphical_next = Graphical_next,
                                     space_prev = Space_prev,
                                     x_position = X_position)
###############################
if CLEF_TO_STENCIL :
  manager.ddls += clef_to_stencil.generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     glyph_idx = Glyph_idx,
                                     glyph_stencil = Glyph_stencil)

###############################
if TIME_SIGNATURE_TO_STENCIL :
  manager.ddls += time_signature_to_stencil.generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     time_signature = Time_signature,
                                     string_box = String_box,
                                     width = Width,
                                     height = Height,
                                     stencil = String_stencil)
###############################
if STAFF_SYMBOL_TO_STENCIL :
  manager.ddls += staff_symbol_to_stencil.generate_ddl(name = Name,
                                     line_thickness = Line_thickness,
                                     n_lines = N_lines,
                                     staff_space = Staff_space,
                                     x_position = X_position,
                                     rhythmic_event_height = Rhythmic_event_height,
                                     line_stencil = Line_stencil)

###############################
if KEY_SIGNATURE_TO_STENCIL :
  manager.ddls += key_signature_to_stencil.generate_ddl(name = Name,
                            font_name = Font_name,
                            font_size = Font_size,
                            key_signature = Key_signature, 
                            width = Width,
                            key_signature_layout_info = Key_signature_layout_info,
                            glyph_stencil = Glyph_stencil)

###############################
if DURATION_LOG_TO_STENCIL :
  manager.ddls += duration_log_to_stencil.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     duration_log = Duration_log,
                                     name = Name,
                                     glyph_stencil = Glyph_stencil)

if not MANUAL_DDL :
  manager.register_ddls(conn, LOG = True)

Score.metadata.drop_all(engine)
Score.metadata.create_all(engine)

emmentaler_tools.populate_glyph_box_table(conn, Glyph_box)
emmentaler_tools.unicode_to_glyph_index_map(conn, Unicode_to_glyph_index_map)
emmentaler_tools.add_to_string_box_table(conn, String_box, '3')
emmentaler_tools.add_to_string_box_table(conn, String_box, '4')
key_signature_tools.populate_key_signature_info_table(conn, Key_signature_layout_info)
conn.execute(duration_log_to_dimension.initialize_dimensions_of_quarter_note(Glyph_box, Rhythmic_event_width, 'width'))
conn.execute(duration_log_to_dimension.initialize_dimensions_of_quarter_note(Glyph_box, Rhythmic_event_height, 'height'))

stmts = []

# DEFAULTS
# TODO - in separate file?
stmts.append((Dot_padding, {'id': -1, 'val':0.1}))
stmts.append((Rhythmic_event_to_dot_padding, {'id':-1, 'val': 0.1}))
stmts.append((Rhythmic_event_to_accidental_padding, {'id':-1, 'val': 0.1}))
stmts.append((Time_signature_inter_number_padding, {'id':-1, 'val': 0.0}))

# link up notes in time
TN = [3,4,5,7,8,None]
for x in range(len(TN) - 1) :
  stmts.append((Time_next, {'id':TN[x], 'val':TN[x+1]}))

# A time signature
stmts.append((Name, {'id':0,'val':'time_signature'}))
stmts.append((Font_name, {'id':0,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':0,'val':20}))
stmts.append((Time_signature, {'id':0,'num':3,'den':4}))

# A key signature
stmts.append((Name, {'id':1,'val':'key_signature'}))
stmts.append((Font_name, {'id':1,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':1,'val':20}))
stmts.append((Key_signature, {'id':1,'val':2}))

# A clef
stmts.append((Name, {'id':2,'val':'clef'}))
stmts.append((Font_name, {'id':2,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':2,'val':20}))
stmts.append((Glyph_idx, {'id':2,'val':116}))

# some notes and rests
stmts.append((Name, {'id':3,'val':'note'}))
stmts.append((Font_name, {'id':3,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':3,'val':20}))
stmts.append((Duration_log, {'id':3,'val':-2}))
stmts.append((Dots, {'id':3,'val':1}))
stmts.append((Accidental, {'id':3,'val':-1}))

stmts.append((Name, {'id':4,'val':'rest'}))
stmts.append((Font_name, {'id':4,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':4,'val':20}))
stmts.append((Duration_log, {'id':4,'val':-1}))
stmts.append((Dots, {'id':4,'val':2}))

stmts.append((Name, {'id':5,'val':'note'}))
stmts.append((Font_name, {'id':5,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':5,'val':20}))
stmts.append((Duration_log, {'id':5,'val':0}))

# another clef
stmts.append((Name, {'id':6,'val':'clef'}))
stmts.append((Font_name, {'id':6,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':6,'val':20}))
stmts.append((Glyph_idx, {'id':6,'val':116}))

# some notes and rests
stmts.append((Name, {'id':7,'val':'note'}))
stmts.append((Font_name, {'id':7,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':7,'val':20}))
stmts.append((Duration_log, {'id':7,'val':-3}))
stmts.append((Dots, {'id':7,'val':2}))
stmts.append((Accidental, {'id':7,'val':1}))

stmts.append((Name, {'id':8,'val':'rest'}))
stmts.append((Font_name, {'id':8,'val':'emmentaler-20'}))
stmts.append((Font_size, {'id':8,'val':20}))
stmts.append((Duration_log, {'id':8,'val':-1}))

# link up things out of time, including to their anchors
# link up notes in time
HT_0 = [2,1,0,None]
for x in range(len(HT_0) - 1) :
  stmts.append((Horstemps_next, {'id':HT_0[x], 'val':HT_0[x+1]}))
  stmts.append((Horstemps_anchor, {'id':HT_0[x], 'val':3}))

HT_1 = [6,None]
for x in range(len(HT_1) - 1) :
  stmts.append((Horstemps_next, {'id':HT_1[x], 'val':HT_1[x+1]}))
  stmts.append((Horstemps_anchor, {'id':HT_1[x], 'val':7}))

# make a staff symbol
stmts.append((Name, {'id':9,'val':'staff_symbol'}))
stmts.append((Line_thickness, {'id':9, 'val':0.5}))
stmts.append((N_lines, {'id':9,'val':5}))
stmts.append((Staff_space, {'id':9, 'val':1.0}))

# run!

trans = conn.begin()
for st in stmts :
  #print "~~~~~~~~~~~~~~~~~~~~~~~", st[0].name, st[1]
  manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
trans.commit()


'''
OUT = {}
for table in TABLES_TO_REPORT :
  #print "!+"*40
  #print "reporting on", table.name
  #print "$%"*40
  #for row in conn.execute(select([table])).fetchall() :
  #  print row
  sub_d = {'columns':table.c.keys(),'rows':[]}
  for row in conn.execute(select([table])).fetchall() :
    sub_d['rows'].append(list(row))
  OUT[table.name] = sub_d

print json.dumps(OUT)
'''

_HOST_NAME = '' 
_PORT_NUMBER = 8000

class Engraver(object) :
  def __init__(self, conn) :
    self.conn = conn
  def engrave(self, sql) :
    result = self.conn.execute(text(sql))
    out = []
    for row in result.fetchall() :
      out.append(list(row))
    return json.dumps(out)

server_class = simple_http_server.MyServer
httpd = server_class((_HOST_NAME, _PORT_NUMBER), simple_http_server.MyHandler, engraver=Engraver(conn))
print time.asctime(), "Server Starts - %s:%s" % (_HOST_NAME, _PORT_NUMBER)
try:
  httpd.serve_forever()
except KeyboardInterrupt:
  pass
httpd.server_close()
print time.asctime(), "Server Stops - %s:%s" % (_HOST_NAME, _PORT_NUMBER)
