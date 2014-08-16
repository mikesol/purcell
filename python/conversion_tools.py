from sqlalchemy import case

def int_to_unicode(v) :
  data = [(v == x, "U+E08{0}".format(x)) for x in range(10)]
  return case(data)
