import tuplet_to_factor
import rhythmic_events_to_durations
import clef_to_width
import staff_position_to_y_position
import key_signature_to_width
import time_signature_to_width
import accidental_to_width
import dots_to_width
import rhythmic_events_to_right_width
import rhythmic_events_to_left_width
import nexts_to_graphical_next
import graphical_next_to_space_prev
import space_prev_to_x_position
import duration_log_to_dimension
import note_head_to_staff_position
import ledger_line_to_line_stencil
import note_head_to_ledger_line
import duration_log_to_stem_length
import duration_log_to_stem_direction
import stem_direction_to_stem_x_offset
import stem_to_line_stencil
import stem_to_stem_end

import staff_symbol_to_stencil
import clef_to_stencil
import time_signature_to_stencil
import key_signature_to_stencil
import duration_log_to_stencil
import duration_log_to_flag_stencil

import bravura_tools
import simple_http_server

from plain import *
from properties import *
from sqlalchemy import create_engine
from sqlalchemy import event, DDL, text

from sqlalchemy.exc import ResourceClosedError

import json
import time
import sys
import re

### websocket fun!
from gevent import monkey; monkey.patch_all()
#from ws4py.websocket import EchoWebSocket
from ws4py.websocket import WebSocket
from ws4py.server.geventserver import WSGIServer
from ws4py.server.wsgiutils import WebSocketWSGIApplication
from ws4py.manager import WebSocketManager
###

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
CLEF_TO_Y_POSITION = T
NOTE_HEAD_TO_STAFF_POSITION = T
NOTE_HEAD_TO_Y_POSITION = T
KEY_SIGNATURE_TO_WIDTH = T
TIME_SIGNATURE_TO_WIDTH = T
ACCIDENTAL_TO_WIDTH = T
DOTS_TO_WIDTH = T
RHYTHMIC_EVENTS_TO_RIGHT_WIDTH = T
RHYTHMIC_EVENTS_TO_LEFT_WIDTH = T
NEXTS_TO_GRAPHICAL_NEXT  = F
GRAPHICAL_NEXT_TO_SPACE_PREV = T
SPACE_PREV_TO_X_POSITION  = T
DURATION_LOG_TO_WIDTH = T
DURATION_LOG_TO_HEIGHT = T
STAFF_SYMBOL_TO_STENCIL = T
CLEF_TO_STENCIL = T
TIME_SIGNATURE_TO_STENCIL = T
KEY_SIGNATURE_TO_STENCIL = T
DURATION_LOG_TO_STENCIL = T
LEDGER_LINE_TO_LINE_STENCIL = T
NOTE_HEAD_TO_LEDGER_LINE = T
DURATION_LOG_TO_STEM_LENGTH = T
DURATION_LOG_TO_STEM_DIRECTION = T
STEM_DIRECTION_TO_STEM_X_OFFSET = T
STEM_TO_STEM_END = T
STEM_TO_LINE_STENCIL = T
DURATION_LOG_TO_FLAG_STENCIL = T

#engine = create_engine('postgresql://localhost/postgres', echo=False)
engine = create_engine('sqlite:///memory', echo=ECHO)
CONN = engine.connect()

generate_sqlite_functions(CONN)

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
                                     key_signature_inter_accidental_padding = Key_signature_inter_accidental_padding,
                                     glyph_box = Glyph_box,
                                     width = Width)

###############################
if CLEF_TO_Y_POSITION :
  manager.ddls += staff_position_to_y_position.generate_ddl(needle='clef',
                                     name = Name,
                                     staff_position = Staff_position,
                                     staff_symbol = Staff_symbol,
                                     staff_space = Staff_space,
                                     y_position = Y_position)

###############################
if NOTE_HEAD_TO_STAFF_POSITION :
  manager.ddls += note_head_to_staff_position.generate_ddl(name = Name,
                                     pitch = Pitch,
                                     octave = Octave,
                                     graphical_next = Graphical_next,
                                     staff_position = Staff_position)

###############################
if NOTE_HEAD_TO_Y_POSITION :
  manager.ddls += staff_position_to_y_position.generate_ddl(needle='note',
                                     name = Name,
                                     staff_position = Staff_position,
                                     staff_symbol = Staff_symbol,
                                     staff_space = Staff_space,
                                     y_position = Y_position)

############################
if DURATION_LOG_TO_STEM_LENGTH :
  manager.ddls += duration_log_to_stem_length.generate_ddl(
                                     duration_log = Duration_log,
                                     name = Name,
                                     stem_length = Stem_length)

#######################
if DURATION_LOG_TO_STEM_DIRECTION :
  manager.ddls += duration_log_to_stem_direction.generate_ddl(staff_position = Staff_position,
                                     stem_length = Stem_length,
                                     stem_direction = Stem_direction)

#######################
if STEM_DIRECTION_TO_STEM_X_OFFSET :
  manager.ddls += stem_direction_to_stem_x_offset.generate_ddl(note_head_width = Note_head_width,
                                stem_direction=  Stem_direction,
                                stem_x_offset = Stem_x_offset)

###############################
if STEM_TO_STEM_END :
  manager.ddls += stem_to_stem_end.generate_ddl(
                                     stem_direction = Stem_direction,
                                     stem_length = Stem_length,
                                     staff_symbol = Staff_symbol,
                                     staff_space = Staff_space,
                                     stem_end = Stem_end)

###############################
if CLEF_TO_WIDTH : 
  manager.ddls += clef_to_width.generate_ddl(name = Name,
                                   font_name = Font_name,
                                   font_size = Font_size,
                                   unicode = Unicode,
                                   glyph_box = Glyph_box,
                                   width = Width)

###############################
if TIME_SIGNATURE_TO_WIDTH : 
  manager.ddls += time_signature_to_width.generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     time_signature = Time_signature,
                                     glyph_box = Glyph_box,
                                     width = Width)

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
                                     rhythmic_event_dimension = Note_head_width,
                                     dimension = 'width')

################################
if DURATION_LOG_TO_HEIGHT :
  manager.ddls += duration_log_to_dimension.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     duration_log = Duration_log,
                                     glyph_box = Glyph_box,
                                     name = Name,
                                     rhythmic_event_dimension = Note_head_height,
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
  manager.ddls += rhythmic_events_to_right_width.generate_ddl(note_head_width = Note_head_width,
                                     dot_width = Dot_width,
                                     rhythmic_event_to_dot_padding = Rhythmic_event_to_dot_padding,
                                     right_width = Right_width)

###############################
if RHYTHMIC_EVENTS_TO_LEFT_WIDTH :
  manager.ddls += rhythmic_events_to_left_width.generate_ddl(note_head_width = Note_head_width,
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
                                     unicode = Unicode,
                                     glyph_stencil = Glyph_stencil)

###############################
if TIME_SIGNATURE_TO_STENCIL :
  manager.ddls += time_signature_to_stencil.generate_ddl(name = Name,
                                     font_name = Font_name,
                                     font_size = Font_size,
                                     time_signature = Time_signature,
                                     glyph_box = Glyph_box,
                                     width = Width,
                                     staff_symbol = Staff_symbol,
                                     staff_space = Staff_space,
                                     stencil = Glyph_stencil)
###############################
if STAFF_SYMBOL_TO_STENCIL :
  manager.ddls += staff_symbol_to_stencil.generate_ddl(name = Name,
                                     line_thickness = Line_thickness,
                                     n_lines = N_lines,
                                     staff_space = Staff_space,
                                     x_position = X_position,
                                     line_stencil = Line_stencil)

###############################
if KEY_SIGNATURE_TO_STENCIL :
  manager.ddls += key_signature_to_stencil.generate_ddl(name = Name,
                            font_name = Font_name,
                            font_size = Font_size,
                            key_signature = Key_signature, 
                            width = Width,
                            staff_symbol = Staff_symbol,
                            staff_space = Staff_space,
                            glyph_stencil = Glyph_stencil)

###############################
if DURATION_LOG_TO_STENCIL :
  manager.ddls += duration_log_to_stencil.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     duration_log = Duration_log,
                                     name = Name,
                                     glyph_stencil = Glyph_stencil)

###############################
if DURATION_LOG_TO_FLAG_STENCIL :
  manager.ddls += duration_log_to_flag_stencil.generate_ddl(font_name = Font_name,
                                     font_size = Font_size,
                                     duration_log = Duration_log,
                                     stem_x_offset = Stem_x_offset,
                                     stem_end = Stem_end,
                                     stem_direction = Stem_direction, 
                                     glyph_stencil = Glyph_stencil)

###############################
if STEM_TO_LINE_STENCIL :
  manager.ddls += stem_to_line_stencil.generate_ddl(
                                     stem_x_offset = Stem_x_offset,
                                     stem_end = Stem_end,
                                     line_stencil = Line_stencil)

###############################
if LEDGER_LINE_TO_LINE_STENCIL :
  manager.ddls += ledger_line_to_line_stencil.generate_ddl(name = Name,
                                     ledger_line = Ledger_line,
                                     n_lines = N_lines,
                                     staff_space = Staff_space,
                                     staff_symbol = Staff_symbol,
                                     note_head_width = Note_head_width,
                                     y_position = Y_position,
                                     line_stencil = Line_stencil)

###############################
if NOTE_HEAD_TO_LEDGER_LINE :
  manager.ddls += note_head_to_ledger_line.generate_ddl(name = Name,
                                     staff_position = Staff_position,
                                     ledger_line = Ledger_line)


if not MANUAL_DDL :
  manager.register_ddls(CONN, LOG = True)

Score.metadata.drop_all(engine)
Score.metadata.create_all(engine)

bravura_tools.populate_glyph_box_table(CONN, Glyph_box)

stmts = []

# DEFAULTS
# TODO - in separate file?
stmts.append((Dot_padding, {'id': -1, 'val':0.1}))
stmts.append((Rhythmic_event_to_dot_padding, {'id':-1, 'val': 0.1}))
stmts.append((Rhythmic_event_to_accidental_padding, {'id':-1, 'val': 0.1}))
stmts.append((Time_signature_inter_number_padding, {'id':-1, 'val': 0.0}))
stmts.append((Key_signature_inter_accidental_padding, {'id':-1, 'val': 0.5}))

'''
# link up notes in time
TN = [3,4,5,7,8,None]
for x in range(len(TN) - 1) :
  stmts.append((Time_next, {'id':TN[x], 'val':TN[x+1]}))
'''

# A time signature
stmts.append((Name, {'id':0,'val':'time_signature'}))
stmts.append((Font_name, {'id':0,'val':'Bravura'}))
stmts.append((Font_size, {'id':0,'val':20}))
stmts.append((Time_signature, {'id':0,'num':3,'den':4}))

# A key signature
stmts.append((Name, {'id':1,'val':'key_signature'}))
stmts.append((Font_name, {'id':1,'val':'Bravura'}))
stmts.append((Font_size, {'id':1,'val':20}))
stmts.append((Key_signature, {'id':1,'val':2}))

# A clef
stmts.append((Name, {'id':2,'val':'clef'}))
stmts.append((Font_name, {'id':2,'val':'Bravura'}))
stmts.append((Font_size, {'id':2,'val':20}))
stmts.append((Unicode, {'id':2,'val':"U+E050"}))
stmts.append((Staff_position, {'id':2,'val':-1.0}))
stmts.append((Pitch, {'id':2,'val':4}))
stmts.append((Octave, {'id':2,'val':0}))

# some notes and rests
stmts.append((Name, {'id':3,'val':'note'}))
stmts.append((Font_name, {'id':3,'val':'Bravura'}))
stmts.append((Font_size, {'id':3,'val':20}))
stmts.append((Duration_log, {'id':3,'val':-2}))
stmts.append((Dots, {'id':3,'val':1}))
stmts.append((Accidental, {'id':3,'val':-1}))
stmts.append((Pitch, {'id':3,'val':1}))
stmts.append((Octave, {'id':3,'val':2}))

stmts.append((Name, {'id':4,'val':'rest'}))
stmts.append((Font_name, {'id':4,'val':'Bravura'}))
stmts.append((Font_size, {'id':4,'val':20}))
stmts.append((Duration_log, {'id':4,'val':-1}))
stmts.append((Dots, {'id':4,'val':2}))

stmts.append((Name, {'id':5,'val':'note'}))
stmts.append((Font_name, {'id':5,'val':'Bravura'}))
stmts.append((Font_size, {'id':5,'val':20}))
stmts.append((Duration_log, {'id':5,'val':0}))
stmts.append((Pitch, {'id':5,'val':4}))
stmts.append((Octave, {'id':5,'val':0}))

# another clef
stmts.append((Name, {'id':6,'val':'clef'}))
stmts.append((Font_name, {'id':6,'val':'Bravura'}))
stmts.append((Font_size, {'id':6,'val':20}))
stmts.append((Unicode, {'id':6,'val':"U+E062"}))
stmts.append((Staff_position, {'id':6,'val':1.0}))
stmts.append((Pitch, {'id':6,'val':3}))
stmts.append((Octave, {'id':6,'val':-1}))

# some notes and rests
stmts.append((Name, {'id':7,'val':'note'}))
stmts.append((Font_name, {'id':7,'val':'Bravura'}))
stmts.append((Font_size, {'id':7,'val':20}))
stmts.append((Duration_log, {'id':7,'val':-3}))
stmts.append((Dots, {'id':7,'val':2}))
stmts.append((Accidental, {'id':7,'val':1}))
stmts.append((Pitch, {'id':7,'val':0}))
stmts.append((Octave, {'id':7,'val':-2}))

stmts.append((Name, {'id':8,'val':'rest'}))
stmts.append((Font_name, {'id':8,'val':'Bravura'}))
stmts.append((Font_size, {'id':8,'val':20}))
stmts.append((Duration_log, {'id':8,'val':-1}))

NEXT = [None, 2,1,0,3,4,5,6,7,8,None]
for x in range(1, len(NEXT) - 1) :
  stmts.append((Graphical_next, {'id' : NEXT[x], 'next' : NEXT[x + 1], 'prev' : NEXT[x-1]}))

'''
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
'''
# make a staff symbol
stmts.append((Name, {'id':9,'val':'staff_symbol'}))
stmts.append((Line_thickness, {'id':9, 'val':0.13}))
stmts.append((N_lines, {'id':9,'val':5}))
stmts.append((Staff_space, {'id':9, 'val':1.0}))

for x in range(9) :
  stmts.append((Staff_symbol, {'id':x,'val':9}))

for x in range(10) :
  stmts.append((Used_ids, {'id':x}))

# run!

trans = CONN.begin()
for st in stmts :
  print "~~~~~~~~~~~~~~~~~~~~~~~", st[0].name, st[1]
  manager.insert(CONN, st[0].insert().values(**st[1]), MANUAL_DDL)
trans.commit()

#for row in CONN.execute(select([Space_prev])).fetchall() : print row
#print "*"*80
#for row in CONN.execute(select([X_position])).fetchall() : print row
#print "*"*80
#for row in CONN.execute(select([Glyph_stencil])).fetchall() : print row

'''
OUT = {}
for table in TABLES_TO_REPORT :
  #print "!+"*40
  #print "reporting on", table.name
  #print "$%"*40
  #for row in CONN.execute(select([table])).fetchall() :
  #  print row
  sub_d = {'columns':table.c.keys(),'rows':[]}
  for row in conn.execute(select([table])).fetchall() :
    sub_d['rows'].append(list(row))
  OUT[table.name] = sub_d

print json.dumps(OUT)
'''

_HOST_NAME = '' 
_PORT_NUMBER = 8000

'''
class Engraver(object) :
  def __init__(self, CONN) :
    self.conn = CONN
  def engrave(self, sql) :
    out = []
    group_transactions = ('INSERT' in sql) | ('UPDATE' in sql) | ('DELETE' in sql)
    sql = sql.split(";")
    trans = None
    if group_transactions :
      trans = self.conn.begin()
    for stmt in sql :
      result = self.conn.execute(text(stmt+";"))
      try :
        for row in result.fetchall() :
          out.append(list(row))
      except ResourceClosedError as e :
        print stmt, "does not return rows, blocking error"
    if group_transactions :
      trans.commit()
    return json.dumps(out)

server_class = simple_http_server.MyServer
httpd = server_class((_HOST_NAME, _PORT_NUMBER), simple_http_server.MyHandler, engraver=Engraver(CONN))
print time.asctime(), "Server Starts - %s:%s" % (_HOST_NAME, _PORT_NUMBER)
try:
  httpd.serve_forever()
except KeyboardInterrupt:
  pass
httpd.server_close()
print time.asctime(), "Server Stops - %s:%s" % (_HOST_NAME, _PORT_NUMBER)
'''

WSM = {}

class Engraver(WebSocket) :
  def received_message(self, JSON) :
    jobj = json.loads(str(JSON))
    if self not in WSM.values() :
      WSM[jobj['client']] = self
    out = {}
    for obj in jobj['sql'] :
      print "evaluating :::", obj['sql']
      result = CONN.execute(text(obj['sql']))
      if obj['expected'] != [] :
        out[obj['name']] = []
        for row in result.fetchall() :
          to_append = {}
          for x in range(len(row)) :
            to_append[obj['expected'][x]] = row[x]
          out[obj['name']].append(to_append)
      if jobj.has_key('subsequent') :
        out['subsequent'] = jobj['subsequent']
    for key in WSM.keys() :
      if jobj.has_key('return') :
        if (jobj['return'] == "*") or re.match(jobj['return'], key) :
          WSM[key].send(json.dumps(out), False)

server = WSGIServer((_HOST_NAME, _PORT_NUMBER), WebSocketWSGIApplication(handler_cls=Engraver))
print time.asctime(), "Server Starts - %s:%s" % (_HOST_NAME, _PORT_NUMBER)
try:
  server.serve_forever()
except KeyboardInterrupt:
  pass
server.server_close()
print time.asctime(), "Server Stops - %s:%s" % (_HOST_NAME, _PORT_NUMBER)
