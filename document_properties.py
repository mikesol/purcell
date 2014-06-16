from blackmagick import IntegerPattern, FractionPattern, StringPattern

def P(_13) :
  #&&&&&&&&&&&&&&&#
  Pointer = _13.make_pointer_table(_13.metadata)
  #&&&&&&&&&&&&&&&#
  Score = _13.make_property('score', IntegerPattern, _13.metadata)
  Staff = _13.make_property('staff', IntegerPattern, _13.metadata)
  Duration = _13.make_property('duration', FractionPattern, _13.metadata)
  Onset = _13.make_property('onset', FractionPattern, _13.metadata)
  End = _13.make_property('end', FractionPattern, _13.metadata)
  Name = _13.make_property('name', StringPattern, _13.metadata)
  Glyph_index = _13.make_property('glyph_index', IntegerPattern, _13.metadata)
  Duration_log = _13.make_property('duration_log', IntegerPattern, _13.metadata)
  Clef_type = _13.make_property('clef_type', StringPattern, _13.metadata)

  return [kls.__table__ for kls in
          [Pointer, Score, Staff, Duration, Onset, End,
           Name, Glyph_index, Duration_log,
           Clef_type]]
