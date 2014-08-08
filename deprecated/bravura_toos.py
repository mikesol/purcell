import freetype
from freetype.ft_errors import FT_Exception
import sys
import xml.dom.minidom as minidom

def populate_glyph_box_table(conn, table, from_cache = False, make_cache = False) :
  unicode_to_glyph_box = unicode_to_glyph_index_map()
  if not from_cache :
    face = freetype.Face("fonts/Bravura.otf")
    face.set_char_size( 20 << 6 )
  # devnull
  CACHE = []
  if not from_cache :
    for key in unicode_to_glyph_box.keys() :
      face.load_glyph(key)
      bbox = face.glyph.outline.get_bbox()
      xMin = bbox.xMin
      xMax = bbox.xMax
      yMin = bbox.yMin
      yMax = bbox.yMax
      uc = unicode_to_glyph_box[key]
      CACHE.append({'name' : "Bravura", 'unicode' : str(uc), 'x' : xMin, 'width' : xMax - xMin, 'y' : yMin, 'height' : yMax - yMin})
  else :
    bravura_cache = file('bravura-cache.txt', 'r')
    cached_values = bravura_cache.read().split('\n')
    cached_values.remove('')
    bravura_cache.close()
    for line in cached_values :
      elts = line.split(' ')
      CACHE.append({'name' : elts[0], 'unicode' : int(elts[1]), 'x' : int(elts[2]), 'width' : int(elts[3]), 'y' : int(elts[4]), 'height' : int(elts[5])})
  trans = conn.begin()
  for elt in CACHE :
    #print elt
    conn.execute(table.insert().values(**elt))
  trans.commit()
  if make_cache :
    bravura_cache = file('bravura-cache.txt', 'w')
    for elt in CACHE :
      bravura_cache.write(' '.join([str(elt[x]) for x in 'name unicode x width y height'.split(' ')]) + '\n')
    bravura_cache.close()

def add_to_string_box_table(conn, table, gnirts) :
  face = freetype.Face("fonts/Bravura.otf")
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
  conn.execute(table.insert().values(name = "Bravura", str = gnirts, x = xMin, width = width, y = yMin, height = yMax - yMin))

def unicode_to_glyph_index_map() :
  dom = minidom.parse('fonts/Bravura.svg')
  svg = filter(lambda x : x.nodeName == 'svg', dom.childNodes)[-1]
  defs = filter(lambda x : x.nodeName == 'defs', svg.childNodes)[-1]
  font = filter(lambda x : x.nodeName == 'font', defs.childNodes)[-1]
  glyphs = filter(lambda x : x.nodeName == 'glyph', font.childNodes)
  CACHE = {}
  USED_IDXS = []
  face = freetype.Face("fonts/Bravura.otf")
  face.set_char_size( 20 << 6 )
  for glyph in glyphs :
    glyph_name = glyph.getAttribute('glyph-name')
    idx = face.get_name_index(str(glyph_name))
    #print str(glyph_name), idx
    # should only be the case for the null glyph
    if idx not in USED_IDXS :
      uc = glyph_name
      if ('uni' in glyph_name) & ("." not in glyph_name) & ("_" not in glyph_name) :
        uc = glyph_name[3:]
        #print "UNICODE FOR", str(glyph_name), idx,":",uc
        CACHE[idx] = uc
        USED_IDXS.append(idx)
  return CACHE
  
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
  
  populate_glyph_box_table(conn, Glyph_box, make_cache = MAKE_CACHE, from_cache = not MAKE_CACHE)
