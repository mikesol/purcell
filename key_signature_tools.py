
def populate_key_signature_info_table(conn, table) :
  # 2.0 * m + b = 0.0
  # 0.0 * m + b = 2.0
  # m = -1
  # b = 2
  INFO = [(2.0,1), (0.5,2),(2.5,3), (1.0,4),(-0.5,5),(1.5,6),(0.0,7)]
  INFO += [(0.0,-1), (1.5,-2),(-0.5,-3), (1.0,-4),(-1.0,-5),(0.5,-6),(-1.5,-7)]
  # rotate to top_of_staff
  INFO = [(elt[0] * -1 + 2, elt[1]) for elt in INFO]
  for elt in INFO :
    conn.execute(table.insert().values(accidental=elt[1],place=elt[0]))
