import freetype
from freetype.ft_errors import FT_Exception
import sys
import xml.dom.minidom as minidom

def populate_glyph_box_table(conn, table, from_cache = False, make_cache = False) :
  if not from_cache :
    face = freetype.Face("fonts/emmentaler-20.otf")
    face.set_char_size( 20 << 6 )
  # devnull
  conn.execute(table.insert().values(name = "emmentaler-20", idx = -1, x = 0, width = 0, y = 0, height = 0))
  x = 0
  CACHE = []
  if not from_cache :
    while True :
      try :
        face.load_glyph(x)
        bbox = face.glyph.outline.get_bbox()
        xMin = bbox.xMin
        xMax = bbox.xMax
        yMin = bbox.yMin
        yMax = bbox.yMax
        CACHE.append({'name' : "emmentaler-20", 'idx' : x, 'x' : xMin, 'width' : xMax - xMin, 'y' : yMin, 'height' : yMax - yMin})
        x += 1
      except FT_Exception as e :
        print e, "at an X of", x, "when counting glyphs in the emmentaler font"
        break  
  else :
    emmentaler_cache = file('emmentaler-cache.txt', 'r')
    cached_values = emmentaler_cache.read().split('\n')
    cached_values.remove('')
    emmentaler_cache.close()
    for line in cached_values :
      elts = line.split(' ')
      CACHE.append({'name' : elts[0], 'idx' : int(elts[1]), 'x' : int(elts[2]), 'width' : int(elts[3]), 'y' : int(elts[4]), 'height' : int(elts[5])})
  trans = conn.begin()
  for elt in CACHE :
    conn.execute(table.insert().values(**elt))
  trans.commit()
  if make_cache :
    emmentaler_cache = file('emmentaler-cache.txt', 'w')
    for elt in CACHE :
      emmentaler_cache.write(' '.join([str(elt[x]) for x in 'name idx x width y height'.split(' ')]) + '\n')
    emmentaler_cache.close()

def add_to_string_box_table(conn, table, gnirts) :
  face = freetype.Face("fonts/emmentaler-20.otf")
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
    bbox = face.glyph.outline.get_bbox()
    temp_xMin = bbox.xMin
    if x == 0 :
      xMin = temp_xMin
    yMin = min(bbox.yMin, yMin)
    width = (bbox.xMax - temp_xMin) + width
    yMax = max(bbox.yMax, yMax)
  conn.execute(table.insert().values(name = "emmentaler-20", str = gnirts, x = xMin, width = width, y = yMin, height = yMax - yMin))

def unicode_to_glyph_index_map(conn, table, from_cache = False, make_cache = False) :
  dom = minidom.parse('fonts/emmentaler-20.svg')
  svg = filter(lambda x : x.nodeName == 'svg', dom.childNodes)[-1]
  defs = filter(lambda x : x.nodeName == 'defs', svg.childNodes)[-1]
  font = filter(lambda x : x.nodeName == 'font', defs.childNodes)[-1]
  glyphs = filter(lambda x : x.nodeName == 'glyph', font.childNodes)
  CACHE = []
  if not from_cache :
    face = freetype.Face("fonts/emmentaler-20.otf")
    face.set_char_size( 20 << 6 )
    for glyph in glyphs :
      glyph_name = glyph.getAttribute('glyph-name')
      idx = face.get_name_index(str(glyph_name))
      #print str(glyph_name), idx
      CACHE.append({'name' : "emmentaler-20", 'idx' : idx, 'unicode' : glyph.getAttribute('unicode')})
  else :
    emmentaler_cache = file('emmentaler-unicode-glyph-index-cache.txt', 'r')
    cached_values = emmentaler_cache.read().split('\n')
    cached_values.remove('')
    emmentaler_cache.close()
    for line in cached_values :
      elts = line.split(' ')
      CACHE.append({'name' : elts[0], 'idx' : int(elts[1]), 'unicode' : unichr(int(elts[2]))})
  trans = conn.begin()
  for elt in CACHE :
    conn.execute(table.insert().values(**elt))
  trans.commit()
  if make_cache :
    emmentaler_cache = file('emmentaler-unicode-glyph-index-cache.txt', 'w')
    for elt in CACHE :
      elt['unicode'] = ord(elt['unicode'])
      emmentaler_cache.write(' '.join([str(elt[x]) for x in 'name idx unicode'.split(' ')]) + '\n')
    emmentaler_cache.close()
  
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
  
  
  unicode_to_glyph_index_map(conn, Unicode_to_glyph_index_map, make_cache = MAKE_CACHE, from_cache = not MAKE_CACHE)
  populate_glyph_box_table(conn, Glyph_box, make_cache = MAKE_CACHE, from_cache = not MAKE_CACHE)
  add_to_string_box_table(conn, String_box, '123')
  add_to_string_box_table(conn, String_box, '12')
  add_to_string_box_table(conn, String_box, '3')
