import rhythmic_events_to_durations
import clef_to_width
import staff_position_to_y_position
import key_signature_to_width
import time_signature_to_width
import accidental_to_width
import dots_to_width
import rhythmic_events_to_right_width
import rhythmic_events_to_left_width
import graphical_next_to_space_prev
import space_prev_to_x_position
import duration_log_to_dimension
import note_head_to_staff_position
import ledger_line_to_line_stencil
import note_head_to_ledger_line
import duration_log_to_stem_length
import duration_log_to_natural_stem_direction
import natural_stem_direction_to_stem_direction
import stem_direction_to_stem_x_offset
import stem_to_line_stencil
import stem_to_natural_stem_end
import natural_stem_end_to_stem_end
import bar_line_to_width
import beam_to_beam_positions
import beam_to_stencil
import rest_to_staff_position
import dynamic_to_staff_position
import dynamic_to_unicode
import dynamic_to_alignment_directive
import rhythmic_head_width_to_note_box
import anchor_to_dimensioned_anchor

import position_to_anchored_position

import staff_symbol_to_stencil
import generic_simple_glyph_writer
import time_signature_to_stencil
import key_signature_to_stencil
import duration_log_to_stencil
import duration_log_to_flag_stencil
import accidental_to_stencil
import dots_to_stencil
import bar_line_to_stencil
import beam_to_stencil

import bravura_tools
import simple_http_server

from core_tools import *
from properties import *
from sqlalchemy import create_engine
from sqlalchemy import event, DDL, text

from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.schema import CreateTable

import json
import time
import sys
import re

def output_to_sql() :

  LOG = False
  ECHO = False
  MANUAL_DDL = False

  T = True
  F = False

  RHYTHMIC_EVENTS_TO_DURATIONS = T
  CLEF_TO_WIDTH = T
  STAFF_POSITION_TO_Y_POSITION = T
  REST_TO_STAFF_POSITION = T
  NOTE_HEAD_TO_STAFF_POSITION = T
  KEY_SIGNATURE_TO_WIDTH = T
  TIME_SIGNATURE_TO_WIDTH = T
  ACCIDENTAL_TO_WIDTH = T
  DOTS_TO_WIDTH = T
  RHYTHMIC_EVENTS_TO_RIGHT_WIDTH = T
  RHYTHMIC_EVENTS_TO_LEFT_WIDTH = T
  RHYTHMIC_HEAD_WIDTH_TO_NOTE_BOX = T
  GRAPHICAL_NEXT_TO_SPACE_PREV = T
  SPACE_PREV_TO_X_POSITION  = T
  DURATION_LOG_TO_WIDTH = T
  DURATION_LOG_TO_HEIGHT = T
  STAFF_SYMBOL_TO_STENCIL = T
  CLEF_TO_STENCIL = T
  DYNAMIC_TO_STENCIL = T
  TIME_SIGNATURE_TO_STENCIL = T
  KEY_SIGNATURE_TO_STENCIL = T
  DURATION_LOG_TO_STENCIL = T
  LEDGER_LINE_TO_LINE_STENCIL = T
  NOTE_HEAD_TO_LEDGER_LINE = T
  DURATION_LOG_TO_STEM_LENGTH = T
  DURATION_LOG_TO_NATURAL_STEM_DIRECTION = T
  NATURAL_STEM_DIRECTION_TO_STEM_DIRECTION = T
  STEM_DIRECTION_TO_STEM_X_OFFSET = T
  STEM_TO_NATURAL_STEM_END = T
  NATURAL_STEM_END_TO_STEM_END = T
  STEM_TO_LINE_STENCIL = T
  DURATION_LOG_TO_FLAG_STENCIL = T
  BEAM_TO_BEAM_X_POSITIONS = T
  BEAM_TO_BEAM_Y_POSITIONS = T
  ACCIDENTAL_TO_STENCIL = T
  DOTS_TO_STENCIL = T
  BAR_LINE_TO_WIDTH = T
  BAR_LINE_TO_STENCIL = T
  BEAM_TO_STENCIL = T
  DYNAMIC_TO_STAFF_POSITION = T
  ANCHOR_TO_DIMENSIONED_ANCHOR = T
  POSITION_TO_ANCHORED_POSITION = T
  DYNAMIC_TO_ALIGNMENT_DIRECTIVE = T
  DYNAMIC_TO_UNICODE = T

  engine = create_engine('sqlite:///:memory:', echo=ECHO)
  CONN = engine.connect()

  #generate_sqlite_functions(CONN)

  manager = DDL_manager()

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
  if BAR_LINE_TO_WIDTH :
    manager.ddls += bar_line_to_width.generate_ddl(
                                       bar_thickness = Bar_thickness,
                                       width = Width)

  ###############################
  if STAFF_POSITION_TO_Y_POSITION :
    manager.ddls += staff_position_to_y_position.generate_ddl(
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
  if REST_TO_STAFF_POSITION :
    manager.ddls += rest_to_staff_position.generate_ddl(
                                       name = Name,
                                       staff_position = Staff_position)
  ############################
  if DURATION_LOG_TO_STEM_LENGTH :
    manager.ddls += duration_log_to_stem_length.generate_ddl(
                                       duration_log = Duration_log,
                                       name = Name,
                                       beam = Beam,
                                       stem_length = Stem_length)

  #######################
  if DURATION_LOG_TO_NATURAL_STEM_DIRECTION :
    manager.ddls += duration_log_to_natural_stem_direction.generate_ddl(name=Name,
           staff_position = Staff_position,
                                       natural_stem_direction = Natural_stem_direction)

  #######################
  if NATURAL_STEM_DIRECTION_TO_STEM_DIRECTION :
    manager.ddls += natural_stem_direction_to_stem_direction.generate_ddl(
                              natural_stem_direction = Natural_stem_direction,
                              beam = Beam,
                              stem_direction = Stem_direction)

  #######################
  if STEM_DIRECTION_TO_STEM_X_OFFSET :
    manager.ddls += stem_direction_to_stem_x_offset.generate_ddl(rhythmic_head_width = Rhythmic_head_width,
                                  stem_direction=  Stem_direction,
                                  stem_x_offset = Stem_x_offset)

  ###############################
  if STEM_TO_NATURAL_STEM_END :
    manager.ddls += stem_to_natural_stem_end.generate_ddl(
                                       stem_direction = Stem_direction,
                                       stem_length = Stem_length,
                                       natural_stem_end = Natural_stem_end)

  #####################################
  if NATURAL_STEM_END_TO_STEM_END :
    manager.ddls += natural_stem_end_to_stem_end.generate_ddl(
                          natural_stem_end = Natural_stem_end,
                                       beam = Beam,
                                       beam_x_position = Beam_x_position,
                                       beam_y_position = Beam_y_position,
                                       x_position = X_position,
                                       stem_x_offset = Stem_x_offset,
                                       staff_position = Staff_position,
                                       stem_end = Stem_end)

  ###############################
  if BEAM_TO_BEAM_X_POSITIONS :
    manager.ddls += beam_to_beam_positions.generate_ddl(
                    stem_direction = Stem_direction,
                                       natural_stem_end = Natural_stem_end,
                                       staff_position = Staff_position,
                                       beam = Beam,
                                       x_position = X_position,
                                       stem_x_offset = Stem_x_offset,
                                       beam_position = Beam_x_position,
                                       x_pos = True)

  ###############################
  if BEAM_TO_BEAM_Y_POSITIONS :
    manager.ddls += beam_to_beam_positions.generate_ddl(
                    stem_direction = Stem_direction,
                                       natural_stem_end = Natural_stem_end,
                                       staff_position = Staff_position,
                                       beam = Beam,
                                       x_position = X_position,
                                       stem_x_offset = Stem_x_offset,
                                       beam_position = Beam_y_position,
                                       x_pos = False)

  ###############################
  if BEAM_TO_STENCIL :
    manager.ddls += beam_to_stencil.generate_ddl(
                                       duration_log = Duration_log,
                                       stem_direction = Stem_direction,
                                       beam = Beam,
                                       beam_x_position = Beam_x_position,
                                       beam_y_position = Beam_y_position,
                                       polygon_stencil = Polygon_stencil)

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
                                       rhythmic_event_dimension = Rhythmic_head_width,
                                       dimension = 'width')

  ################################
  if DURATION_LOG_TO_HEIGHT :
    manager.ddls += duration_log_to_dimension.generate_ddl(font_name = Font_name,
                                       font_size = Font_size,
                                       duration_log = Duration_log,
                                       glyph_box = Glyph_box,
                                       name = Name,
                                       rhythmic_event_dimension = Rhythmic_head_height,
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
    manager.ddls += rhythmic_events_to_right_width.generate_ddl(rhythmic_head_width = Rhythmic_head_width,
                                       dot_width = Dot_width,
                                       rhythmic_event_to_dot_padding = Rhythmic_event_to_dot_padding,
                                       right_width = Right_width)

  ###############################
  if RHYTHMIC_EVENTS_TO_LEFT_WIDTH :
    manager.ddls += rhythmic_events_to_left_width.generate_ddl(rhythmic_head_width = Rhythmic_head_width,
                                       accidental_width = Accidental_width,
                                       rhythmic_event_to_accidental_padding = Rhythmic_event_to_accidental_padding,
                                       left_width = Left_width)

  ###############################
  if RHYTHMIC_HEAD_WIDTH_TO_NOTE_BOX :
    manager.ddls += rhythmic_head_width_to_note_box.generate_ddl(rhythmic_head_width = Rhythmic_head_width,
                          stem_end = Stem_end,
                          staff_position = Staff_position,
                          note_box = Note_box)

  ###############################
  if ANCHOR_TO_DIMENSIONED_ANCHOR :
    manager.ddls += anchor_to_dimensioned_anchor.generate_ddl(anchor=Anchor, anchor_dim = Anchor_x)
    manager.ddls += anchor_to_dimensioned_anchor.generate_ddl(anchor=Anchor, anchor_dim = Anchor_y)


  if POSITION_TO_ANCHORED_POSITION :
    manager.ddls += position_to_anchored_position.generate_ddl(anchor = Anchor_x,
                                       position = X_position,
                                       anchored_position = Anchored_x_position)

    manager.ddls += position_to_anchored_position.generate_ddl(anchor = Anchor_y,
                                       position = Y_position,
                                       anchored_position = Anchored_y_position)
  ###############################
  if DYNAMIC_TO_ALIGNMENT_DIRECTIVE :
    manager.ddls += dynamic_to_alignment_directive.generate_ddl(dynamic_direction = Dynamic_direction,
                       alignment_directive = Alignment_directive)

  ###############################
  if DYNAMIC_TO_UNICODE :
    manager.ddls += dynamic_to_unicode.generate_ddl(dynamic=Dynamic, unicode=Unicode)

  ###############################
  if DYNAMIC_TO_STAFF_POSITION :
    manager.ddls += dynamic_to_staff_position.generate_ddl(dynamic = Dynamic,
                anchor_x = Anchor_x,
                note_box = Note_box,
                dynamic_direction = Dynamic_direction,
                dynamic_padding = Dynamic_padding,
                staff_position = Staff_position)

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
    manager.ddls += space_prev_to_x_position.generate_ddl(
                                       space_prev = Space_prev,
                                       x_position = X_position)
  ###############################
  if CLEF_TO_STENCIL :
    manager.ddls += generic_simple_glyph_writer.generate_ddl(
                                       font_name = Font_name,
                                       font_size = Font_size,
                                       unicode = Unicode,
                                       glyph_box = Glyph_box,
                                       alignment_directive = Alignment_directive,
                                       glyph_stencil = Glyph_stencil,
                                       writer = 'clef_to_stencil',
                                       extra_eq = [Name.c.val == 'clef', Name.c.id == Font_name.c.id]
                                       )

  ###############################
  if DYNAMIC_TO_STENCIL :
    manager.ddls += generic_simple_glyph_writer.generate_ddl(
                                       font_name = Font_name,
                                       font_size = Font_size,
                                       unicode = Unicode,
                                       glyph_box = Glyph_box,
                                       alignment_directive = Alignment_directive,
                                       glyph_stencil = Glyph_stencil,
                                       writer = 'dynamic_to_stencil',
                                       extra_eq = [Dynamic.c.id == Font_name.c.id])

  ###############################
  if TIME_SIGNATURE_TO_STENCIL :
    manager.ddls += time_signature_to_stencil.generate_ddl(
                                       name = Name,
                                       font_name = Font_name,
                                       font_size = Font_size,
                                       time_signature = Time_signature,
                                       glyph_box = Glyph_box,
                                       width = Width,
                                       staff_symbol = Staff_symbol,
                                       staff_space = Staff_space,
                                       stencil = Glyph_stencil)
  ###############################
  if BAR_LINE_TO_STENCIL :
    manager.ddls += bar_line_to_stencil.generate_ddl(name = Name,
                                       bar_thickness = Bar_thickness,
                                       staff_symbol = Staff_symbol,
                                       staff_space = Staff_space,
                                       n_lines = N_lines,
                                       line_stencil = Line_stencil)

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
  if ACCIDENTAL_TO_STENCIL :
    manager.ddls += accidental_to_stencil.generate_ddl(
                                       font_name = Font_name,
                                       font_size = Font_size,
                                       accidental = Accidental,
                                       accidental_width = Accidental_width, 
                                       rhythmic_event_to_accidental_padding = Rhythmic_event_to_accidental_padding,
                                       glyph_stencil = Glyph_stencil)

  ###############################
  if DOTS_TO_STENCIL :
    manager.ddls += dots_to_stencil.generate_ddl(
                                       font_name = Font_name,
                                       font_size = Font_size,
                                       dots = Dots,
                                       dot_width = Dot_width, 
                                       rhythmic_head_width = Rhythmic_head_width,
                                       rhythmic_event_to_dot_padding = Rhythmic_event_to_dot_padding,
                                       glyph_stencil = Glyph_stencil)

  ###############################
  if DURATION_LOG_TO_FLAG_STENCIL :
    manager.ddls += duration_log_to_flag_stencil.generate_ddl(
                                       beam = Beam,
                                       font_name = Font_name,
                                       font_size = Font_size,
                                       duration_log = Duration_log,
                                       stem_x_offset = Stem_x_offset,
                                       natural_stem_end = Natural_stem_end,
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
                                       rhythmic_head_width = Rhythmic_head_width,
                                       y_position = Y_position,
                                       line_stencil = Line_stencil)

  ###############################
  if NOTE_HEAD_TO_LEDGER_LINE :
    manager.ddls += note_head_to_ledger_line.generate_ddl(name = Name,
                                       staff_position = Staff_position,
                                       ledger_line = Ledger_line)


  manager.transitively_reduce_ddl_list(safe_removals = [
    (Staff_position, Beam_x_position),
    (Staff_position, Beam_y_position),
    (Stem_direction, Beam_y_position)
  ])

  OUT = ''
  for table in Score.metadata.sorted_tables :
    OUT += str(CreateTable(table))
    OUT += ";"
    if 'id' in [col.name for col in table.columns] :
      OUT += 'CREATE INDEX ix_{0}_id ON {0} (id);\n'.format(table.name)

  for ddl in manager.ddls :
    holder = ddl.as_ddl(False)
    OUT += holder.instruction

  OUT += bravura_tools.populate_glyph_box_table(None, None)

  return OUT

if __name__ == '__main__' :
  import os
  if not os.path.isdir('../build') :
    os.mkdir('../build')
  SQL = output_to_sql()
  file = file('../build/raw_sql','w')
  file.write(SQL)
  file.close()