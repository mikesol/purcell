
def populate_key_signature_info_table(conn, table) :
  INFO = [(2.0,1), (0.5,2),(2.5,3), (1.0,4),(-0.5,5),(1.5,6),(0.0,7)]
  INFO += [(0.0,-1), (1.5,-2),(-0.5,-3), (1.0,-4),(-1.0,-5),(0.5,-6),(-1.5,-7)]
  for elt in INFO :
    conn.execute(table.insert().values(accidental=elt[1],place=elt[0]))
