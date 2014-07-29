from sqlalchemy import Integer, Float, String
from plain import make_table, Fraction

Score = make_table('score', Integer)
Staff = make_table('staff', Integer)
Voice = make_table('voice', Integer)

### prime tables

# name of the thing (chord, tuplet, etc.)
Name = make_table('name', String(50))

# note to containing chord
Chord = make_table('chord', Integer)
# 2**duration_log is the base length of the note
Duration_log = make_table('duration_log', Integer)
# dots to a note
Dots = make_table('dots', Integer)
# the next chord, rest, or space in a sequence
Time_next = make_table('time_next', Integer, unique=True)
# the anchor for time next strands
Onset_anchor = make_table('onset_anchor', Integer)

# accidental attached to a note
Accidental = make_table('accidental', Fraction)
# pitch attached to a note
Pitch = make_table('pitch', Integer)
# octave attached to a note
Octave = make_table('octave', Integer)

# the fraction of a tuplet
Tuplet_fraction = make_table('tuplet_fraction', Fraction)

# time signature num den
Time_signature = make_table('time_signature', Fraction)

# key signature where +1 is G Major, -4 A-flat major, etc...
Key_signature = make_table('key_signature', Integer)

### derived tables

# the full duration of an event
Duration = make_table('duration', Fraction)

# the onset of an event
Local_onset = make_table('local_onset', Fraction)
# the event in a sequence that is anchored to another sequence
Onset_referent = make_table('onset_referent', Integer)
# the global onset in the piece
Global_onset = make_table('global_onset', Fraction)


Left_bound = make_table('left_bound', Integer)
Right_bound = make_table('right_bound', Integer)
Tuplet_factor = make_table('tuplet_factor', Fraction)

Horstemps_next = make_table('horstemps_next', Integer, unique=True)
Horstemps_anchor = make_table('horstemps_anchor', Integer)

Font_name = make_table('font_name', String)
Font_size = make_table('font_size', Float)
Glyph_idx = make_table('glyph_idx', Integer)

Width = make_table('width', Float)

Graphical_next = make_table('graphical_next', Integer)