from sqlalchemy import case, Column, Integer, select

def E(_13) :
  ####################################
  # assign glyph_index to clefs

  glyph_indexless_clefs =\
    _13.last_outerjoin(
      _13.session.query(_13.Name.id,
                         _13.Clef_type.val),
                       _13.Name,
                       [_13.Clef_type, _13.Glyph_index]).\
      filter(_13.Name.val == "clef").\
      filter(_13.Glyph_index.val == None).\
      with_labels().\
      subquery('glyph_indexless_clefs')

  Glyph_indexless_clefs = _13.reflect(glyph_indexless_clefs, _13.Clef_type)

  glyph_indexless_ands =\
    [(Glyph_indexless_clefs.clef_type__val == "treble", 116),
     (Glyph_indexless_clefs.clef_type__val == "bass", 114),
     (Glyph_indexless_clefs.clef_type__val == "alto", 112)
    ]

  return {
    "inserter" : [_13.insert(_13.Glyph_index,
    select([glyph_indexless_clefs.c.name_id,
            case(glyph_indexless_ands,
                 else_ = 116)]))]
  }
