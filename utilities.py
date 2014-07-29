from sqlalchemy import select, and_, func

def bound_range(first, iter, last) :
  table = select([iter.c.id.label('elt')]).where(iter.c.id == first).cte(name = 'first_row', recursive = True)
  grow_me = table.alias(name = 'grow_me')
  table = table.union_all(select([iter.c.val.label('next')]).\
       where(and_(grow_me.c.elt == iter.c.id,
                  grow_me.c.elt != last)))
  return table

def in_range(first, iter, last, val) :
  stmt = bound_range(first, iter, last)
  stmt_c = stmt.c.values()[0]
  setified = select([stmt]).union(select([val])).cte(name="setified")
  setified_c = setified.c.values()[0]
  setified_count = select([func.count(setified_c)]).cte(name="setified_count")
  setified_count_c = setified_count.c.values()[0]
  statement_count = select([func.count(stmt_c)]).cte(name="statement_count")
  statement_count_c = statement_count.c.values()[0]
  counts = select([setified_count_c.label('n_set'), statement_count_c.label('n_list')]).cte("counts")
  return counts.c.n_set == counts.c.n_list

if __name__ == '__main__' :
  from sqlalchemy import Table, Column, Integer, MetaData
  from sqlalchemy import create_engine

  engine = create_engine('sqlite:///:memory:', echo=False)
  conn = engine.connect()  
  
  metadata = MetaData()
  foo = Table('foo', metadata, Column('id', Integer, primary_key=True), Column('val', Integer))
  bar = Table('bar', metadata, Column('id', Integer, primary_key=True), Column('val', Integer))
  iter = Table('iter', metadata, Column('id', Integer, primary_key=True), Column('val', Integer))
  #print bound_range(0, iter, 100).element

  metadata.create_all(engine)
  conn.execute(iter.insert().values(id=1, val=40))
  conn.execute(iter.insert().values(id=40, val=20))
  conn.execute(iter.insert().values(id=20, val=2))
  conn.execute(iter.insert().values(id=2, val=100))

  conn.execute(foo.insert().values(id=2, val=3))
  conn.execute(foo.insert().values(id=7, val=3))

  print bound_range(1, iter, 100).element
  for r in conn.execute(select([bound_range(1, iter, 100)])).fetchall() :
    print r
  print "***************"
  #print in_range(1, iter, 2, 50)
  #print select([iter]).where(in_range(1, iter, 100, 500))
  #ugh, more = in_range(40, iter, 2, 2)
  #for r in conn.execute(ugh) :
  #  print r
  print "&&&&"
  #print select([foo]).where(in_range(40, iter, 2, 40))
  #for r in conn.execute(select([foo]).where(in_range(40, iter, 2, 500))) :
  #  print r
  #print in_range(40, iter, 2, foo.c.id)
  print select([foo.c.id]).where(in_range(40, iter, 2, foo.c.id))
  for r in conn.execute(select([foo.c.id]).where(in_range(40, iter, 2, foo.c.id))) :
    print r