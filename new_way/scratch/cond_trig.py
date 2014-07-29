from sqlalchemy import Table, MetaData, Column, Integer, DDL
from sqlalchemy import create_engine, event

ECHO = False
engine = create_engine('sqlite:///memory', echo=ECHO)

ddl = DDL(
'''CREATE TRIGGER foo AFTER INSERT ON bar
      BEGIN
          INSERT INTO bar SELECT * FROM foo WHERE 2 % foo.id = 0;
      END;
    '''  
)

md = MetaData()
Foo = Table('foo', md, Column('id', Integer))
Bar = Table('bar', md, Column('id', Integer))

event.listen(Foo, 'after_create', ddl.execute_if(dialect='sqlite'))

Foo.metadata.create_all(engine)
