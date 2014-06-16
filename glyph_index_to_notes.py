from sqlalchemy import case, Column, Integer, select

def E(_13) :
  ####################################
  # assign glyph_index

  glyph_indexless_notes =\
    _13.last_outerjoin(
      _13.session.query(_13.Name.id,
                         _13.Duration_log.val),
                       _13.Name,
                       [_13.Duration_log, _13.Glyph_index]).\
      filter(_13.Name.val == "note").\
      filter(_13.Glyph_index.val == None).\
      with_labels().\
      subquery('glyph_indexless_notes')

  Glyph_indexless_notes = _13.map(glyph_indexless_notes,
                                  "glyph_indexless_notes",
                                  Column('id', Integer, primary_key=True),
                                  _13.Duration_log)

  glyph_indexless_ands =\
    [(Glyph_indexless_notes.duration_log__val == 1, 149),
     (Glyph_indexless_notes.duration_log__val == 2, 148)]

  return {
    "inserter" : [_13.insert(_13.Glyph_index,
    select([glyph_indexless_notes.c.name_id,
            case(glyph_indexless_ands,
                 else_ = 147)]))]
  }
