import freetype
from freetype.ft_errors import FT_Exception
import sys
import xml.dom.minidom as minidom
import json

def _gulp(f) :
  infi = file(f,'r')
  out = infi.read()
  infi.close()
  return out

def populate_glyph_box_table(conn, table) :
  glyphnames = json.loads(_gulp("fonts/glyphnames.json"))
  bravura_metadata = json.loads(_gulp("fonts/bravura_metadata.json"))
  bboxes = bravura_metadata["glyphBBoxes"]
  CACHE = []
  for key in glyphnames.keys() :
    if key in bboxes :
      x = bboxes[key]['bBoxSW'][0]
      y = bboxes[key]['bBoxSW'][1]
      width = bboxes[key]['bBoxNE'][0] - x
      height = bboxes[key]['bBoxNE'][1] - y
      CACHE.append({"name" : "Bravura", "unicode" : str(glyphnames[key]['codepoint']), 'x' : x, 'y': y, 'width':width, 'height':height})
  trans = conn.begin()
  for elt in CACHE :
    conn.execute(table.insert().values(**elt))
  trans.commit()

if __name__ == '__main__' :
  from plain import *
  from sqlalchemy import create_engine
  
  MAKE_CACHE = True
  
  ECHO = True
  #ECHO = False
  MANUAL_DDL = True
  #MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  Glyph_box.metadata.drop_all(engine)
  Glyph_box.metadata.create_all(engine)
  
  populate_glyph_box_table(conn, Glyph_box)