from sqlalchemy import case, Column, Integer, select

def E(_13) :
  ####################################
  # assign clef_type to clefs

  clef_typeless_clefs =\
    _13.last_outerjoin(
      _13.session.query(_13.Name.id),
                       _13.Name,
                       [_13.Clef_type]).\
      filter(_13.Name.val == "clef").\
      filter(_13.Clef_type.val == None).\
      with_labels().\
      subquery('clef_typeless_clefs')

  Clef_typeless_clefs = _13.reflect(clef_typeless_clefs, _13.Clef_type)

  return {
    "inserter" : [_13.insert(_13.Clef_type,
      select([clef_typeless_clefs.c.name_id,
            "'treble'"]))]
  }
