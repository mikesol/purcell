'''
we go from left to right on both skylines, stopping at each point
we find all lines active at that point and the point on these lines
'''

from sqlalchemy import Table, Column, Integer, MetaData, Float
from sqlalchemy import or_, and_, case, select, func
from sqlalchemy import create_engine
from sqlalchemy.exc import ResourceClosedError
    
def _bound_skyline_range(first, skyline, name) :
  start = select([
    skyline.c.id.label('id'),
    skyline.c.x0.label('x0'),
    skyline.c.y0.label('y0'),
    skyline.c.x1.label('x1'),
    skyline.c.y1.label('y1'),
    skyline.c.next.label('next'),
    ]).where(skyline.c.id == first).cte(name=name, recursive=True)
  start_a = start.alias(name=name+'_prev')
  start = start.union_all(select([
    skyline.c.id.label('id'),
    skyline.c.x0.label('x0'),
    skyline.c.y0.label('y0'),
    skyline.c.x1.label('x1'),
    skyline.c.y1.label('y1'),
    skyline.c.next.label('next')]
  ).where(skyline.c.id == start_a.c.next))
  return start

def _make_fully_lined(skyline, all_xs, name, conn) :
  known = select([all_xs.c.x.label('x'), skyline.c.y0.label('y')]).\
                select_from(all_xs.join(skyline, onclause=all_xs.c.x == skyline.c.x0)).\
              union(select([all_xs.c.x, skyline.c.y1]).\
                select_from(all_xs.join(skyline, onclause=all_xs.c.x == skyline.c.x1))).\
                   cte(name=name+'_xy_known')

  if conn :
    print "********"
    print select([known])
    for row in conn.execute(select([known])).fetchall() :
      print row

  unknown = select([all_xs.c.x.label('x')]).\
              select_from(all_xs.outerjoin(known, onclause = all_xs.c.x == known.c.x)).\
                where(known.c.y == None).cte(name=name+'_xy_unknown')

  if conn :
    print "$$$$$$$$"
    print select([unknown])
    for row in conn.execute(select([unknown])).fetchall() :
      print row

  unknown_points = select([unknown.c.x.label('x'), (((unknown.c.x - skyline.c.x0) / (skyline.c.x1 - skyline.c.x0)) * (skyline.c.y1 - skyline.c.y0) + skyline.c.y0).label('y')]).\
       where(case([(skyline.c.x0 < skyline.c.x1, and_(unknown.c.x >= skyline.c.x0, unknown.c.x <= skyline.c.x1))], else_ = and_(unknown.c.x <= skyline.c.x0, unknown.c.x >= skyline.c.x1))).cte(name=name+'xy_new_points')

  if conn :
    print ":::::::::::::"
    print select([unknown_points])
    try :
      for row in conn.execute(select([unknown_points])).fetchall() :
        print row
    except ResourceClosedError :
      print "------------ does not return rows ---------"

  all_known = select([known.c.x.label('x'), known.c.y.label('y')]).union_all(select([unknown_points])).cte(name=name+'_all_known')
  
  if conn :
    print ">>>>>>>>>>>"
    print select([all_known])
    for row in conn.execute(select([all_known])).fetchall() :
      print row

  all = select([all_xs.c.x.label('x'), (func.min if name == 'upper' else func.max)(all_known.c.y).label('y')]).\
     select_from(all_xs.outerjoin(all_known, onclause = all_known.c.x == all_xs.c.x)).group_by(all_xs.c.x).cte(name=name+'_fully_lined')
  
  if conn :
    print "<<<<<<<<<<<<<<"
    print select([all])
    for row in conn.execute(select([all])).fetchall() :
      print row

  return all

def _skyline_differences(upper, lower, skyline, name, conn) :
  upper = _bound_skyline_range(upper, skyline, name+'_upper_bsr')
  lower = _bound_skyline_range(lower, skyline, name+'_lower_bsr')
  all_xs = select([upper.c.x0.label('x')]).cte(name=name+"_all_xs_1").\
    union(select([upper.c.x1]))
  all_xs = select([all_xs.c.x.label('x')]).cte(name=name+"_all_xs_2").\
    union(select([lower.c.x0]))
  all_xs = select([all_xs.c.x.label('x')]).cte(name=name+"_all_xs_3").\
    union(select([lower.c.x1]))
  if conn :
    print "*** upper ****"
    for row in conn.execute(select([upper])).fetchall() :
      print row
  upper_f = _make_fully_lined(upper, all_xs, 'upper', conn)
  lower_f = _make_fully_lined(lower, all_xs, 'lower', conn)
  differences = select([upper_f.c.x.label('x'), case([(and_(upper_f.c.y != None, lower_f.c.y != None), upper_f.c.y - lower_f.c.y)], else_ = None).label('y')]).\
    where(upper_f.c.x == lower_f.c.x).cte(name = name+'_differences')
  if conn :
    print "*** differences ****"
    for row in conn.execute(select([differences])).fetchall() :
      print row
  return differences

def _skyline_difference(upper, lower, skyline, name, conn) :
  diffs = _skyline_differences(upper, lower, skyline, name, conn)
  minn = select([func.min(diffs.c.y).label('diff')]).cte(name = name+'_difference')
  return minn

if __name__ == '__main__' :
  ECHO = False
  engine = create_engine('sqlite:///memory', echo=ECHO)
  conn = engine.connect()

  metadata = MetaData()
  skylines = Table('skylines', metadata,
    Column('id', Integer),
    Column('x0', Float),
    Column('y0', Float),
    Column('x1', Float),
    Column('y1', Float),
    Column('next', Integer))

  metadata.drop_all(engine)
  metadata.create_all(engine)

  P1 = [(0.0, 0.0, 1.0, 0.0), (1.0,0.0,1.0,1.0),(0.0,1.0,1.0,1.0),(0.0,1.0,0.0,0.0)]
  P2 = [(1.0,3.0,0.5,3.0),(1.0,2.0,1.0,3.0),(0.5,3.0,1.0,2.0)]
  #P2 = [(0.5, 0.5, 1.5, 0.5), (1.5,0.5,1.5,1.5),(0.5,1.5,1.5,1.5),(0.5,1.5,0.5,0.5)]
  
  CT = 0
  for polygon in [P1, P2] :
    for x in range(len(polygon)) :
      now = polygon[x]
      conn.execute(skylines.insert().values(id = CT, x0 = now[0], y0 = now[1], x1 = now[2], y1= now[3], next = (CT + 1) if (x < len(polygon) - 1) else None))
      CT += 1
  
  for row in conn.execute(select([skylines])) : print row
  #print _skyline_difference(0, 4, skylines, 'my_diff')
  #print select([_skyline_differences(0, 4, skylines, 'my_diffs', None)])
  #select([_skyline_differences(4, 0, skylines, 'my_diffs', conn)])
  for row in conn.execute(select([_skyline_differences(4, 0, skylines, 'my_diffs', None)])).fetchall() :
    print row

  #select([_skyline_intersect(4, 0, skylines, 'my_diffs', conn)])
  for row in conn.execute(select([_skyline_difference(4, 0, skylines, 'my_diffs', None)])).fetchall() :
    print row