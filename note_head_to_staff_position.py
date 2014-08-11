'''
FIX THIS !!!!
CHOPPING OFF LAST VALUE....
'''
from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time

from functools import partial

# need to find a way to work font size into this...

class _DeleteFromNote(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, name, staff_position) :
    def where_clause_fn(id) :
      stmt = select([name.c.id]).where(and_(staff_position.c.id == id, name.c.id == id, name.c.val == 'note'))
      return exists(stmt)
    DeleteStmt.__init__(self, staff_position, where_clause_fn)

class _DeleteFromClef(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, name, graphical_next, staff_position) :
    def where_clause_fn(id) :
      stmt = select([graphical_next.c.id.label('id')]).\
        where(and_(graphical_next.c.id == id,
                   name.c.id == id,
                   name.c.val == 'clef')).\
        cte(recursive = True, name = "first_clef")
      stmt_a = stmt.alias(name="first_clef_prev")
      stmt = stmt.union_all(
        select([graphical_next.c.next]).\
           where(and_(
                 stmt_a.c.id == graphical_next.c.id,
                 name.c.id == graphical_next.c.next,
                 name.c.val != 'clef'))
      )
      notes = select([stmt.c.id.label('id')]).where(and_(name.c.id == stmt.c.id, name.c.val == 'note', staff_position.c.id == name.c.id))
      #print str(notes)
      return exists(notes)
    DeleteStmt.__init__(self, staff_position, where_clause_fn)

class _InsertNote(InsertStmt) :
  def __init__(self, name, pitch, octave, graphical_next, staff_position) :
    InsertStmt.__init__(self)
    self.name = name
    self.pitch = pitch
    self.octave = octave
    self.graphical_next = graphical_next
    self.staff_position = staff_position
  def _generate_stmt(self, id) :
    #print "@@ON ID", id
    name = self.name
    pitch = self.pitch
    octave = self.octave
    graphical_next = self.graphical_next
    staff_position = self.staff_position

    clef_name = name.alias("clef_name")
    note_name = name.alias("note_name")
    clef_pitch = pitch.alias('clef_pitch')
    note_pitch = pitch.alias('note_pitch')
    clef_octave = octave.alias('clef_octave')
    note_octave = octave.alias('note_octave')
    clef_staff_position = staff_position.alias('staff_position')

    # joins prevent bad pitches
    clef_finder = select([
      graphical_next.c.id.label('id'),
      graphical_next.c.prev.label('prev'),
      case([(clef_name.c.val == 'clef',1)], else_=0).label('is_clef')
    ]).select_from(graphical_next.outerjoin(clef_name, onclause=clef_name.c.id == graphical_next.c.prev)).\
      where(note_name.c.id == graphical_next.c.id).\
      where(note_name.c.val == 'note').\
      where(safe_eq_comp(note_name.c.id, id)).\
       cte(name="clef_finder", recursive=True)
    
    self.register_stmt(clef_finder)
    clef_finder_prev = clef_finder.alias(name="clef_finder_prev")

    clef_finder = clef_finder.union_all(
      select([
        clef_finder_prev.c.prev,
        graphical_next.c.prev,
        case([(clef_name.c.val == 'clef',1)], else_=0)
      ]).where(clef_finder_prev.c.is_clef == 0).\
          where(graphical_next.c.id == clef_finder_prev.c.prev).\
          where(clef_name.c.id == graphical_next.c.prev)
     )

    self.register_stmt(clef_finder)
    '''
    delete_me = select([
      note_pitch.c.id.label('id'),
      ((((note_pitch.c.val + (7 * note_octave.c.val)) - (clef_pitch.c.val + (7 * clef_octave.c.val))) / 2.0) + clef_staff_position.c.val).label('val'),
      note_pitch.c.val.label('np_cv'),
      note_octave.c.val.label('no_cv'),
      clef_pitch.c.val.label('cp_cv'),
      clef_octave.c.val.label('co_cv'),
      clef_staff_position.c.val.label('sp_cv'),
    ]).select_from(clef_finder.\
          join(clef_pitch, onclause=clef_finder.c.prev == clef_pitch.c.id).\
          join(clef_octave, onclause=clef_pitch.c.id == clef_octave.c.id).\
          join(clef_staff_position, onclause=clef_pitch.c.id == clef_staff_position.c.id)
          ).\
       where(note_octave.c.id == note_pitch.c.id).\
       where(clef_finder.c.is_clef == True).\
       where(safe_eq_comp(note_pitch.c.id, id)).cte(name="delete_me")

    self.register_stmt(delete_me)
    '''
    note_to_staff_position = select([
      note_pitch.c.id.label('id'),
      ((((note_pitch.c.val + (7 * note_octave.c.val)) - (clef_pitch.c.val + (7 * clef_octave.c.val))) / 2.0) + clef_staff_position.c.val).label('val'),
    ]).select_from(clef_finder.\
          join(clef_pitch, onclause=clef_finder.c.prev == clef_pitch.c.id).\
          join(clef_octave, onclause=clef_pitch.c.id == clef_octave.c.id).\
          join(clef_staff_position, onclause=clef_pitch.c.id == clef_staff_position.c.id)
          ).\
       where(note_octave.c.id == note_pitch.c.id).\
       where(clef_finder.c.is_clef == True).\
       where(safe_eq_comp(note_pitch.c.id, id)).cte(name="note_to_staff_position")

    self.register_stmt(note_to_staff_position)

    self.insert = simple_insert(staff_position, note_to_staff_position)

class _InsertClef(InsertStmt) :
  def __init__(self, name, pitch, octave, graphical_next, staff_position) :
    InsertStmt.__init__(self)
    self.name = name
    self.pitch = pitch
    self.octave = octave
    self.graphical_next = graphical_next
    self.staff_position = staff_position
  def _generate_stmt(self, id) :
    #print "@@ON ID", id
    name = self.name
    pitch = self.pitch
    octave = self.octave
    graphical_next = self.graphical_next
    staff_position = self.staff_position

    first_clef_name = name.alias("first_clef_name")
    last_clef_name = name.alias("last_clef_name")
    note_name = name.alias("note_name")
    clef_pitch = pitch.alias('clef_pitch')
    note_pitch = pitch.alias('note_pitch')
    clef_octave = octave.alias('clef_octave')
    note_octave = octave.alias('note_octave')
    clef_staff_position = staff_position.alias('staff_position')

    # joins prevent bad pitches
    notes_after_clef = select([
      graphical_next.c.id.label('id'),
      graphical_next.c.next.label('next'),
      case([(last_clef_name.c.val == 'clef',1)], else_=0).label('is_clef')
    ]).select_from(graphical_next.outerjoin(last_clef_name, onclause=last_clef_name.c.id == graphical_next.c.next)).\
      where(first_clef_name.c.id == graphical_next.c.id).\
      where(first_clef_name.c.val == 'clef').\
      where(safe_eq_comp(first_clef_name.c.id, id)).\
       cte(name="notes_after_clef", recursive=True)
    
    self.register_stmt(notes_after_clef)
    notes_after_clef_next = notes_after_clef.alias(name="notes_after_clef_next")

    notes_after_clef = notes_after_clef.union_all(
      select([
        notes_after_clef_next.c.next,
        graphical_next.c.next,
        case([(last_clef_name.c.id == 'clef',1)], else_=0)
      ]).where(notes_after_clef_next.c.is_clef == 0).\
          where(graphical_next.c.id == notes_after_clef_next.c.next).\
          where(last_clef_name.c.id == graphical_next.c.next)
     )

    self.register_stmt(notes_after_clef)

    just_notes = select(
       [notes_after_clef.c.id.label('id')]
     ).where(notes_after_clef.c.id == note_name.c.id).\
        where(note_name.c.val == 'note').cte(name="just_notes")

    self.register_stmt(just_notes)

    note_to_staff_position = select([
      just_notes.c.id.label('id'),
      ((((note_pitch.c.val + (7 * note_octave.c.val)) - (clef_pitch.c.val + (7 * clef_octave.c.val))) / 2.0) + clef_staff_position.c.val).label('val')
    ]).select_from(just_notes.\
          join(note_pitch, onclause=just_notes.c.id == note_pitch.c.id).\
          join(note_octave, onclause=just_notes.c.id == note_octave.c.id)).\
       where(clef_octave.c.id == clef_pitch.c.id).\
       where(clef_octave.c.id == clef_staff_position.c.id).\
       where(safe_eq_comp(clef_pitch.c.id, id)).cte(name="note_to_staff_position")

    self.register_stmt(note_to_staff_position)

    self.insert = simple_insert(staff_position, note_to_staff_position)

def generate_ddl(name, pitch, octave, graphical_next, staff_position) :
  OUT = []

  insert_stmt_note = _InsertNote(name, pitch, octave, graphical_next, staff_position)
  insert_stmt_clef = _InsertClef(name, pitch, octave, graphical_next, staff_position)

  del_stmt_note = _DeleteFromNote(name, staff_position)
  del_stmt_clef = _DeleteFromClef(name, graphical_next, staff_position)

  OUT += [DDL_unit(table, action, [del_stmt_note], [insert_stmt_note])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, pitch, octave, graphical_next]]

  # uggghhhh - probably very time consuming
  OUT += [DDL_unit(table, action, [del_stmt_clef], [insert_stmt_clef])
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [name, pitch, octave, graphical_next, staff_position]]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  MANUAL_DDL = True
  #MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(name = Name,
                                     pitch = Pitch,
                                     octave = Octave,
                                     graphical_next = Graphical_next,
                                     staff_position = Staff_position))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  #bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  stmts.append((Name, {'id':0,'val':'clef'}))
  stmts.append((Pitch, {'id':0,'val':4}))
  stmts.append((Octave, {'id':0,'val':0}))
  stmts.append((Staff_position, {'id':0,'val':-1.0}))
  stmts.append((Graphical_next, {'id':0,'prev':None, 'next':1}))

  for x in range(1,20) :
    stmts.append((Name, {'id':x,'val':'note'}))
    stmts.append((Pitch, {'id':x,'val':x % 7}))
    stmts.append((Octave, {'id':x,'val':0}))
    next = (x + 1) if (x < 19) else None
    stmts.append((Graphical_next, {'id':x,'prev':x-1, 'next':next}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Staff_position])).fetchall() :
    print row
  
  manager.update(conn, Pitch.update().values(**{'val':3}).where(Pitch.c.id == 0), MANUAL_DDL)
  
  print "*************"
  print time.time() - NOW
  for row in conn.execute(select([Staff_position])).fetchall() :
    print row
