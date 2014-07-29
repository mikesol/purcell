import freetype
from freetype.ft_errors import FT_Exception

def populate_glyph_box_table(conn, table) :
  face = freetype.Face("/Users/mikesolomon/Library/Fonts/emmentaler-20.otf")
  face.set_char_size( 20 << 6 )
  # devnull
  conn.execute(table.insert().values(name = "emmentaler-20", idx = -1, x = 0, width = 0, y = 0, height = 0))
  x = 0
  while True :
    try :
      face.load_glyph(x)
      #face.load_glyph(x, freetype.FT_LOAD_NO_SCALE)
      #print face.units_per_EM
      xMin = face.glyph.outline.get_bbox().xMin
      xMax = face.glyph.outline.get_bbox().xMax
      yMin = face.glyph.outline.get_bbox().yMin
      yMax = face.glyph.outline.get_bbox().yMax
      conn.execute(table.insert().values(name = "emmentaler-20", idx = x, x = xMin, width = xMax - xMin, y = yMin, height = yMax - yMin))
      x += 1
    except FT_Exception :
      break  

def add_to_string_box_table(conn, table, gnirts) :
  face = freetype.Face("/Users/mikesolomon/Library/Fonts/emmentaler-20.otf")
  face.set_char_size( 20 << 6 )
  x = 0
  xMin = 0
  yMin = 0
  width = 0
  yMax = 0
  for x in range(len(gnirts)) :
    ch = gnirts[x]
    face.load_char(ch)
    #face.load_glyph(x, freetype.FT_LOAD_NO_SCALE)
    #print face.units_per_EM
    temp_xMin = face.glyph.outline.get_bbox().xMin
    if x == 0 :
      xMin = temp_xMin
    yMin = min(face.glyph.outline.get_bbox().yMin, yMin)
    width = (face.glyph.outline.get_bbox().xMax - temp_xMin) + width
    yMax = max(face.glyph.outline.get_bbox().yMax, yMax)
  conn.execute(table.insert().values(name = "emmentaler-20", str = gnirts, x = xMin, width = width, y = yMin, height = yMax - yMin))

if __name__ == '__main__' :
  from plain import *
  from sqlalchemy import create_engine
  
  ECHO = True
  MANUAL_DDL = True
  #MANUAL_DDL = False
  #engine = create_engine('postgresql://localhost/postgres', echo=False)
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()
  generate_sqlite_functions(conn)

  Glyph_box.metadata.drop_all(engine)
  Glyph_box.metadata.create_all(engine)
  
  populate_glyph_box_table(conn, Glyph_box)
  add_to_string_box_table(conn, String_box, '123')
  add_to_string_box_table(conn, String_box, '12')
  add_to_string_box_table(conn, String_box, '3')
