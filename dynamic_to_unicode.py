from sqlalchemy.sql.expression import literal, distinct, exists, text, case
from plain import *
import time
import bravura_tools
from staff_transform import staff_transform

# need to find a way to work font size into this...

class _Delete(DeleteStmt) :
  #def __init__(self, width, name) :
  def __init__(self, unicode) :
    def where_clause_fn(id) :
      return unicode.c.id == id
    DeleteStmt.__init__(self, unicode, where_clause_fn)

class _Insert(InsertStmt) :
  def __init__(self, dynamic, unicode) :
    InsertStmt.__init__(self)
    self.dynamic = dynamic
    self.unicode = unicode

  def _generate_stmt(self, id) :
    dynamic = self.dynamic
    unicode = self.unicode
    
    dynamic_to_unicode = select([
      dynamic.c.id.label('id'),
      case([(dynamic.c.val == 'pppppp', "U+E527"),
        (dynamic.c.val == 'ppppp', "U+E528"),
        (dynamic.c.val == 'pppp', "U+E529"),
        (dynamic.c.val == 'ppp', "U+E52A"),
        (dynamic.c.val == 'pp', "U+E52B"),
        (dynamic.c.val == 'p', "U+E520"),
        (dynamic.c.val == 'mp', "U+E52C"),
        (dynamic.c.val == 'mf', "U+E52D"),
        (dynamic.c.val == 'p', "U+E522"),
        (dynamic.c.val == 'pf', "U+E52E"),
        (dynamic.c.val == 'f', "U+E522"),
        (dynamic.c.val == 'ff', "U+E52F"),
        (dynamic.c.val == 'fff', "U+E530"),
        (dynamic.c.val == 'ffff', "U+E531"),
        (dynamic.c.val == 'fffff', "U+E532"),
        (dynamic.c.val == 'ffffff', "U+E533"),
        (dynamic.c.val == 'fp', "U+E534"),
        (dynamic.c.val == 'fz', "U+E535"),
        (dynamic.c.val == 'sf', "U+E536"),
        (dynamic.c.val == 'sfp', "U+E537"),
        (dynamic.c.val == 'sfpp', "U+E538"),
        (dynamic.c.val == 'sfz', "U+E539"),
      ])
    ]).where(safe_eq_comp(dynamic.c.id, id)).\
    cte(name='dynamic_to_unicode')

    self.register_stmt(dynamic_to_unicode)

    self.insert = simple_insert(unicode, dynamic_to_unicode)

def generate_ddl(dynamic, unicode) :
  OUT = []

  insert_stmt = _Insert(dynamic, unicode)

  del_stmt = _Delete(unicode)
  
  when = None
  
  OUT += [DDL_unit(table, action, [del_stmt], [insert_stmt], when_clause = when)
     for action in ['INSERT', 'UPDATE', 'DELETE']
     for table in [dynamic]]

  return OUT

if __name__ == "__main__" :
  import math
  from properties import *
  from sqlalchemy import create_engine
  from sqlalchemy import event, DDL
  
  ECHO = False
  #MANUAL_DDL = True
  MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  manager = DDL_manager(generate_ddl(
    dynamic = Dynamic,
    unicode = Unicode))

  if not MANUAL_DDL :
    manager.register_ddls(conn, LOG = True)

  Score.metadata.drop_all(engine)
  Score.metadata.create_all(engine)

  bravura_tools.populate_glyph_box_table(conn, Glyph_box)

  stmts = []

  for x in range(5) :
    stmts.append((Dynamic, {'id':x, 'val':["ppp","p","mf","fff","mp"][x]}))

  trans = conn.begin()
  for st in stmts :
    manager.insert(conn, st[0].insert().values(**st[1]), MANUAL_DDL)
  trans.commit()

  NOW = time.time()
  for row in conn.execute(select([Unicode])).fetchall() :
    print row
  
  #manager.update(conn, Duration, {'num':100, 'den':1}, Duration.c.id == 4, MANUAL_DDL)
  
  #print "*************"
  #print time.time() - NOW
  #for row in conn.execute(select([Local_onset])).fetchall() :
  #  print row
