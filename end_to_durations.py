def E(_13) :

  ####################################
  # assign end to all onsets from durations

  endless_onsets = _13.outerjoin(_13.session.query(_13.Onset.id, _13.Onset.val), _13.Onset, [_13.End]).\
      filter(_13.End.val == None).cte('endless_onsets')

  onset_duration = _13.add(endless_onsets, _13.Duration, hint=_13.Duration)

  return {
    "inserter" : [_13.insert(_13.End, onset_duration)],
  }

