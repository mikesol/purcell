from sqlalchemy import Table, Integer, Float, String, Column, func, alias, select, insert, update, union_all
from sqlalchemy.orm import mapper
from sqlalchemy.orm import composite, column_property, relationship
from abc import ABCMeta, abstractmethod
from fractions import Fraction
from sqlalchemy.orm.properties import CompositeProperty
from sqlalchemy.orm import sessionmaker
from functools import reduce
from types import MethodType
import time

import sys
import string
_ALIASES = 100

def _get_with_fallback(d,k,v) :
  return v if not d.has_key(k) else d[k]

def aliasedclass(fn) :
  def new_fn(*args, **kwargs) :
    kls = fn(*args, **kwargs)
    kls._a = [aliased(kls) for x in range(_ALIASES)]
    return kls
  return new_fn

def _get_table(n) :
  if isinstance(n, Table) :
    return n
  elif hasattr(n, '__table__') :
    return n.__table__
  elif type(n) == type(select().cte()) :
    return n
  else :
    print n.__class__
    print n
    raise ValueError("ugggghhhhhhhh", n)

def _get_table_or_constant(n) :
  if isinstance(n, Table) :
    return n
  elif hasattr(n, '__table__') :
    return n.__table__
  else :
    return n

class _MappedClass(object) : pass

class Pattern(object) :
  __metaclass__ = ABCMeta
  @classmethod
  @abstractmethod
  def make_table(kls, name, metadata) : assert(1==0)
  @staticmethod
  @abstractmethod
  def make_kls(name) : assert(1==0)
  @staticmethod
  @abstractmethod
  def make_mappings(table, kls) : assert (1==0)

class _Singleton(_MappedClass) :
  def __init__(self, id, val) :
    self.id = id
    self.val = val

class SingletonPattern(Pattern) :
  ops = ['add','sub','mul','div']
  kls = None
  @classmethod
  def make_table(kls, name, metadata) :
    return Table(name, metadata,
       Column('id', Integer, primary_key=True),
       Column('val', kls.kls))
  @staticmethod
  def make_kls(name) :
    return type(string.capitalize(name), (_Singleton,), {})
  @staticmethod
  def make_mappings(table, kls) :
    return {}
  @staticmethod
  def op(table1, table2, _op) :
    return select([table1.c.id, _op(table1.c.val, table2.c.val)]).\
      select_from(table1.join(table2, onclause = table1.c.id == table2.c.id))
  @staticmethod
  def add(table1, table2) :
    return SingletonPattern.op(table1, table2, lambda x,y : x+y)
  @staticmethod
  def sub(table1, table2) :
    return SingletonPattern.op(table1, table2, lambda x,y : x-y)
  @staticmethod
  def mul(table1, table2) :
    return SingletonPattern.op(table1, table2, lambda x,y : x*y)
  @staticmethod
  def div(table1, table2) :
    return SingletonPattern.op(table1, table2, lambda x,y : x/y)

class IntegerPattern(SingletonPattern) :
  kls = Integer

class FloatPattern(SingletonPattern) :
  kls = Float

class StringPattern(SingletonPattern) :
  kls = String

class _Fraction(_MappedClass) :
  def __init__(self, num=None, den=None, id=None) :
    self.id = id
    self.num = num
    self.den = den
  @staticmethod
  def _frac(blob) :
    if isinstance(blob, _Fraction) :
      return Fraction(blob.num, blob.den)
    elif isinstance(blob, Fraction) :
      return blob
    elif type(blob) == type(0) :
      return blob
    elif type(blob) == type(0.0) :
      return blob
    else :
      raise ValueError("Don't know how to deal with this... "+str(blob))
  def __gt__(self, other) :
    return _Fraction._frac(self) > _Fraction._frac(other)
  def __lt__(self, other) :
    return _Fraction._frac(self) < _Fraction._frac(other)
  def __ge__(self, other) :
    return _Fraction._frac(self) >= _Fraction._frac(other)
  def __le__(self, other) :
    return _Fraction._frac(self) <= _Fraction._frac(other)
  def __eq__(self, other) :
    return _Fraction._frac(self) < _Fraction._frac(other)
  def __ne__(self, other) :
    return _Fraction._frac(self) != _Fraction._frac(other)
  def __add__(self, other) :
    return _Fraction._frac(self) + _Fraction._frac(other)
  def __sub__(self, other) :
    return _Fraction._frac(self) - _Fraction._frac(other)
  def __mul__(self, other) :
    return _Fraction._frac(self) * _Fraction._frac(other)
  def __div__(self, other) :
    return _Fraction._frac(self) / _Fraction._frac(other)

class FractionComparator(CompositeProperty.Comparator) :
  @staticmethod
  def numden(blob) :
    if hasattr(blob, '__clause_element__') :
      clauses = blob.__clause_element__().clauses
      num = BlackMagick.col(clauses, 'num')
      den = BlackMagick.col(clauses, 'den')
      return num, den
    elif isinstance(blob, Fraction) :
      return blob.numerator, blob.denominator
    elif blob ==  None :
      return None, None
    else :
      raise ValueError('cannot determine type of blob: '+blob.__class__)
  @staticmethod
  def singleton(blob) :
    num, den = FractionComparator.numden(blob)
    if (num is None) | (den is None) :
      return None
    return 1.0 * num / den
  def __lt__(self, other) :
    return FractionComparator.singleton(self) <\
      FractionComparator.singleton(other)
  def __gt__(self, other) :
    return FractionComparator.singleton(self) >\
      FractionComparator.singleton(other)
  def __le__(self, other) :
    return FractionComparator.singleton(self) <=\
      FractionComparator.singleton(other)
  def __ge__(self, other) :
    return FractionComparator.singleton(self) >=\
      FractionComparator.singleton(other)
  def __eq__(self, other) :
    if other is None :
      num, den = FractionComparator.numden(self)
      return (num == None) | (den == None)
    return FractionComparator.singleton(self) ==\
      FractionComparator.singleton(other)
  def __ne__(self, other) :
    return FractionComparator.singleton(self) !=\
      FractionComparator.singleton(other)
    
class FractionPattern(Pattern) :
  ops = ['add','sub','mul','div']
  kls = Integer
  @classmethod
  def make_table(kls, name, metadata) :
    return Table(name, metadata,
       Column('id', Integer, primary_key=True),
       Column('num', kls.kls),
       Column('den', kls.kls))
  @staticmethod
  def make_kls(name) :
    return type(string.capitalize(name), (_Fraction,), {})
  @staticmethod
  def make_mappings(table, kls) :
    return {'val': composite(_Fraction, table.c.num, table.c.den,
                             comparator_factory = FractionComparator),
            'min': column_property(func.min(1.0 * table.c.num / table.c.den)),
            'max': column_property(func.max(1.0 * table.c.num / table.c.den))}
  @staticmethod
  def gcd_table(table) :
    table_al = table.alias(name='fraction_gcd_first')

    modulo = select([table_al.c.id.label('id'),
                    table_al.c.id.label('raw_id'),
                    table_al.c.num.label('num'),
                    table_al.c.den.label('den')]).\
                        where(table_al.c.id == table.c.id).\
                        cte('fraction_gcd', recursive=True)

    modulo_a = modulo.alias(name="fraction_gcd_next")

    modulo = modulo.union_all(
        select([
            (modulo_a.c.id + 1).label('id'),
            modulo_a.c.raw_id,
            (modulo_a.c.den % modulo_a.c.num).label('prev'),
            modulo_a.c.num.label('next')
        ]).\
            where(modulo_a.c.num > 0)
    )

    stmt = select([table.c.id.label('id'), (table.c.num / modulo.c.den).label('num'), (table.c.den / modulo.c.den).label('den')]).\
      select_from(
        table.\
          join(modulo, onclause=table.c.id == modulo.c.raw_id)).order_by(modulo.c.id).group_by(modulo.c.raw_id)

    return stmt
  @staticmethod
  def op(table1, table2, num, den, op_name) :
    return FractionPattern.gcd_table(
      select([table1.c.id.label('id'),
              num.label('num'),
              den.label('den')]).\
        select_from(table1.join(table2, onclause = table1.c.id==table2.c.id)).\
        cte('fraction_'+op_name))
  @staticmethod
  def add(table1, table2) :
    return FractionPattern.op(table1, table2,
         (table1.c.num * table2.c.den) + (table2.c.num * table1.c.den),
         table1.c.den * table2.c.den, 'add')
  @staticmethod
  def sub(table1, table2) :
    return FractionPattern.op(table1, table2,
         (table1.c.num * table2.c.den) - (table2.c.num * table1.c.den),
         table1.c.den * table2.c.den, 'sub')
  @staticmethod
  def mul(table1, table2) :
    return FractionPattern.op(table1, table2,
         table1.c.num * table2.c.num,
         table1.c.den * table2.c.den, 'mul')
  @staticmethod
  def div(table1, table2) :
    return FractionPattern.op(table1, table2,
         table1.c.num * table2.c.den,
         table1.c.den * table2.c.num, 'div')

class _PropertyHolder(_MappedClass) :
  props = None
  def __init__(self, *args, **kwargs) :
    for x in range(len(args)) :
      setattr(self, props[x], args[x])

class BlackMagick(object) :
  def __init__(self) :
    self._properties = []
    self._patterns = {}
  @staticmethod
  def t(blob) :
    return _get_table(blob)
  @staticmethod
  def col_names(blob) :
    t = _get_table(blob)
    return [c.name for c in t.c]
  @staticmethod
  def insert(blob, stmt) :
    return BlackMagick.t(blob).insert().from_select(
      BlackMagick.col_names(blob),
      stmt)
  def make_pointer_table(self, metadata) :
    c_id = Column('id', Integer, primary_key=True)
    c_sr = Column('source', String(50))
    table = Table('pointer', metadata,
       c_id,
       c_sr)
    kls_dict = {'id' : c_id, 'source' : c_sr}
    # ugh, props interface kludgy but needed for the init function...
    kls_dict['props'] = ['id','source']
    kls = type("POINTER", (_PropertyHolder,), kls_dict)
    kls.__table__ = table
    mapper(kls, table)
    return kls
  def make_counter(self) :
    stmts = [select([func.count(prop[1]).label('sum_of_property')]) for prop in self._properties]
    united = union_all(*stmts)
    return select([func.sum(united.c['sum_of_property'])])
  @staticmethod
  def make_pointer_assigner(to_assign, pointer) :
    u1 = update(to_assign.__table__).values(pointer =  select([func.count(pointer.id)]))
    u2 = update(to_assign.__table__).values(pointer = to_assign.__table__.c.pointer + to_assign.__table__.c.id)
    u3 = insert(pointer.__table__).from_select(['id','source'], select([to_assign.__table__.c.pointer, "'{0}'".format(to_assign.__class__.__name__)]))
    return [u1, u2, u3]
  def make_updater(self, engraver) :
    cols = [prop.split('__') for prop in filter(lambda x : "__" in x, engraver.__table__.c.keys())]
    col_d = {}
    for col in cols :
      if not col_d.has_key(col[0]) :
        col_d[col[0]] = []
      col_d[col[0]].append(col[1])
    out = []
    for key, value in col_d.items() :
      table = filter(lambda x : x.name == key, [prop[1] for prop in self._properties])[0]
      tcols = [table.c[col] for col in value]
      ecols = [engraver.__table__.c[key+"__"+col] for col in value]
      out.append(insert(table).from_select(value, select(ecols)))
    return out
  @aliasedclass
  def make_property(self, name, pattern, metadata) :
    table = pattern.make_table(name, metadata)
    kls = pattern.make_kls(name)
    kls.__table__ = table
    properties = pattern.make_mappings(table, kls)
    mapper(kls, table, properties=properties)
    self._properties.append((kls,table,properties))
    if pattern not in self._patterns.values() :
      self._add_pattern(pattern)
    self._patterns[table] = pattern
    return kls
  def _add_pattern(self, pattern) :
    for method in pattern.ops :
      if not hasattr(self, '_'+method+'s') :
        setattr(self, '_'+method+'s', {})
        def closure(use_me=method) :
          def pattern_method(inst, n, *args, **kwargs) :
            hint = kwargs['hint'] if kwargs.has_key('hint') else n
            fn = getattr(inst, '_'+use_me+'s')[
                inst._patterns[_get_table(hint)]
              ]
            return fn(_get_table(n), *[_get_table_or_constant(x) for x in args])
          return pattern_method
        setattr(self, method,
          MethodType(closure(), self, type(self)))
      getattr(self, '_'+method+'s')[pattern] = getattr(pattern, method)
  def map(self, table_or_metadata, name, *args) :
    '''
    can either return a mapping to a new table if a metatable is provided
    or a link to a table if the table is a table
    '''
    column_names = []
    columns = []
    properties = {}
    for arg in args :
      if isinstance(arg, Column) :
        columns.append(arg)
        column_names.append(arg.name)
      elif hasattr(arg.__table__.c, 'val') :
        cname = arg.__table__.name+"__val"
        if isinstance(table_or_metadata, MetaData) :
          columns.append(Column(cname, arg.__table__.c.val.type))
        else :
          columns.append(BlackMagick.col(table_or_metadata.c, arg.__table__.name, 'val'))
        column_names.append(cname)
      else :
        property = filter(lambda x : x[0] == arg, self._properties)[0]
        val = property[2]['val']
        composite_columns = []
        if isinstance(table_or_metadata, MetaData) :
          composite_columns += [Column(arg.__table__.name+"__"+c.name, c.type) for c in val.attrs]
        else :
          composite_columns += [BlackMagick.col(table_or_metadata.c, arg.__table__.name, c.name) for c in val.attrs]
        column_names += [arg.__table__.name+"__"+c.name for c in val.attrs]
        properties[arg.__table__.name+'__val'] = composite(val.composite_class, *composite_columns,
                 comparator_factory=val.comparator_factory)
        columns += composite_columns
    kls_dict = {column_names[x]: columns[x] for x in range(len(columns))}
    # ugh, props interface kludgy but needed for the init function...
    kls_dict['props'] = column_names[:]
    kls = type(name, (_PropertyHolder,), kls_dict)
    table = table_or_metadata
    if isinstance(table, MetaData) :
      table = Table(string.lower(name), metadata, *columns)
      kls.__table__ = table
    mapper(kls, table, properties=properties, primary_key=table.c.values()[0])
    #mapper(kls, table, primary_key=table.c.values()[0])
    return kls
  def names(self, *args) :
    # ugh...code dup of above...clean this up
    column_names = []
    for arg in args :
      if isinstance(arg, Column) :
        column_names.append(arg.name)
      elif hasattr(arg.__table__.c, 'val') :
        cname = arg.__table__.name+"__val"
        column_names.append(cname)
      else :
        property = filter(lambda x : x[0] == arg, self._properties)[0]
        val = property[2]['val']
        column_names += [arg.__table__.name+"__"+c.name for c in val.attrs]
    return column_names
  @staticmethod
  def col(cols, *args) :
    values = cols
    if type(cols) == type({}) :
      values = cols.values()
    for value in values :
      if reduce(lambda x,y : x&y, [arg in value.name for arg in args], True):
        return value
    return None
  def join(self, query, base, to_join, outer=False) :
    out = query
    for elt in to_join :
      out = getattr(out, 'outerjoin' if outer else 'join')(elt, elt.id == base.id)
    return out
  def outerjoin(self, query, base, to_join) :
    return self.join(query, base, to_join, True)
  def last_outerjoin(self, query, base, to_join) :
    return self.join(query, base, to_join[:-1]).outerjoin(to_join[-1], to_join[-1].id == base.id)
    

if __name__ == "__main__" :
  from sqlalchemy import MetaData, create_engine, asc, desc, insert, update, select, and_, delete, case
  from sqlalchemy.orm import aliased
  from sqlalchemy.schema import CreateTable

  #&&&&&&&&&&&&&&&#
  engine = create_engine('sqlite:///:memory:', echo=False)
  _13 = BlackMagick()
  metadata = MetaData()
  #&&&&&&&&&&&&&&&#

  SQL = {}

  #&&&&&&&&&&&&&&&#
  POINTER = _13.make_pointer_table(metadata)
  #&&&&&&&&&&&&&&&#
  Score = _13.make_property('score', IntegerPattern, metadata)
  Staff = _13.make_property('staff', IntegerPattern, metadata)
  Duration = _13.make_property('duration', FractionPattern, metadata)
  Onset = _13.make_property('onset', FractionPattern, metadata)
  End = _13.make_property('end', FractionPattern, metadata)
  Name = _13.make_property('name', StringPattern, metadata)
  Glyph_index = _13.make_property('glyph_index', IntegerPattern, metadata)
  Duration_log = _13.make_property('duration_log', IntegerPattern, metadata)
  Clef_type = _13.make_property('clef_type', StringPattern, metadata)

  SQL['properties'] = [POINTER.__table__]+[prop[1] for prop in _13._properties]
  SQL['counter'] = _13.make_counter()

  #&&&&&&&&&&&&&&&#

  #&&&&&&&&&&&&&&&#
  Session = sessionmaker()
  conn = engine.connect()
  session = Session(bind=conn)
  #&&&&&&&&&&&&&&&#

  SQL['engravers'] = []
  ####################################
  # assign first clef to all staves
  first_events_on_staffs =\
    _13.join(session.query(Score._a[0].val, Staff._a[0].val, Onset._a[0].val, Onset._a[0].min.label('onset_min')),
             Score._a[0], [Staff._a[0], Onset._a[0]]).\
      group_by(Score._a[0].val, Staff._a[0].val).\
      order_by('onset_min').\
      with_labels().\
      subquery(name="first_events_on_staffs")

  first_clefs_on_staffs =\
    _13.join(session.query(Score._a[1].val, Staff._a[1].val, Onset._a[1].val, Onset._a[1].min.label('onset_min'), Name._a[1]),
             Score._a[1], [Staff._a[1], Onset._a[1], Name._a[1]]).\
      filter(Name._a[1].val == "clef").\
      group_by(Score._a[1].val, Staff._a[1].val).\
      order_by('onset_min').\
      with_labels().\
      subquery(name="first_celfs_on_staffs")

  clef_instr = [Score, Staff, Onset, Column('onset_min', Float)]

  First_events_on_staffs = _13.map(first_events_on_staffs, "First_events_on_staffs", *clef_instr)
  First_clefs_on_staffs = _13.map(first_clefs_on_staffs, "First_clefs_on_staffs", *(clef_instr+[Name]))
  
  joined_firsts_to_clefs = session.query(First_events_on_staffs.score__val,
                             First_events_on_staffs.staff__val,
                             First_events_on_staffs.onset__val,
                             "'clef'").outerjoin(
    First_clefs_on_staffs,
      and_(First_events_on_staffs.score__val == First_clefs_on_staffs.score__val,
           First_events_on_staffs.staff__val == First_clefs_on_staffs.staff__val,
           First_events_on_staffs.onset__val == First_clefs_on_staffs.onset__val)).\
    filter(First_clefs_on_staffs.name__val == None).subquery(name="joined_firsts_to_clefs")

  First_clef_engraver = _13.map(metadata, "First_clef_engraver",
     Column('id', Integer, primary_key=True),
     Score, Staff, Onset, Name, Column('pointer', Integer))

  clef_insert = _13.t(First_clef_engraver).insert().from_select(
       _13.names(Score, Staff, Onset, Name),
       joined_firsts_to_clefs)

  SQL['engravers'].append({
    "tables" : [_13.t(First_clef_engraver)],
    "inserter" : [clef_insert],
    "pointer" : _13.make_pointer_assigner(First_clef_engraver, POINTER),
    "updater" : _13.make_updater(First_clef_engraver)+[delete(_13.t(First_clef_engraver))]
  })
  ####################################
  # assign end to all onsets from durations

  endless_onsets = _13.outerjoin(session.query(Onset.id, Onset.val), Onset, [End]).\
      filter(End.val == None).cte('endless_onsets')

  onset_duration = _13.add(endless_onsets, Duration, hint=Duration)

  SQL['engravers'].append({
    "inserter" : [_13.insert(End, onset_duration)],
  })

  ####################################
  # assign clef type

  typeless_clefs = _13.outerjoin(session.query(Name.id, Name.val), Name, [Clef_type]).\
      filter(Name.val == "clef").filter(Clef_type.val == None).cte('typeless_clefs')

  SQL['engravers'].append({
    "inserter" : [_13.insert(Clef_type,
    select([typeless_clefs.c.id, "'treble'"]))]
  })

  ####################################
  # assign duration_log to notes

  duration_logless_notes = _13.last_outerjoin(session.query(Name.id, Duration.val), Name, [Duration, Duration_log]).\
      filter(Name.val == "note").filter(Duration_log.val == None).with_labels().subquery('duration_logless_notes')

  Duration_logless_notes = _13.map(duration_logless_notes, "duration_logless_notes", Column('id', Integer, primary_key=True), Duration)

  duration_logless_ands =\
    [(and_(Duration_logless_notes.duration__val >= Fraction(2,1)**(x-2),
      Duration_logless_notes.duration__val < Fraction(2,1)**(x-1)), x)
    for x in range(-7,7)]

  SQL['engravers'].append({
    "inserter" : [_13.insert(Duration_log,
    select([duration_logless_notes.c.name_id,
            case(duration_logless_ands,
                 else_ = 147)]))]
  })

  ####################################
  # assign glyph_index

  glyph_indexless_notes = _13.last_outerjoin(session.query(Name.id, Duration_log.val), Name, [Duration_log, Glyph_index]).\
      filter(Name.val == "note").filter(Glyph_index.val == None).with_labels().subquery('glyph_indexless_notes')

  Glyph_indexless_notes = _13.map(glyph_indexless_notes, "glyph_indexless_notes", Column('id', Integer, primary_key=True), Duration_log)

  glyph_indexless_ands =\
    [(Glyph_indexless_notes.duration_log__val == 1, 149),
     (Glyph_indexless_notes.duration_log__val == 2, 148)]

  SQL['engravers'].append({
    "inserter" : [_13.insert(Glyph_index,
    select([glyph_indexless_notes.c.name_id,
            case(glyph_indexless_ands,
                 else_ = 147)]))]
  })

  #------------------%%%%%%%%%%%%%%%%%

  ####################################
  # Logic
  # first, create tables
  for PROPERTY in SQL['properties'] :
    engine.execute(CreateTable(PROPERTY))

  #&&&&&&&&&&&&&&&#
  # then, populate them
  INSERTS = []
  INSERTS += [Score(id=x+1, val=0) for x in range(8)]
  INSERTS += [Staff(id=x+1, val=x%2) for x in range(8)]
  INSERTS += [Duration(id=x+1, num=1, den=4) for x in range(8)]
  INSERTS += [Onset(id=x+1, num=x/2, den=4) for x in range(8)]
  INSERTS += [Name(id=x+1, val="note") for x in range(8)]
  INSERTS += [POINTER(id=x+1, source=None) for x in range(8)]
  for x in INSERTS :
    session.add(x)
  session.commit()
  #&&&&&&&&&&&&&&&#
  
  #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#
  # then, create engravers
  for ENGRAVER in SQL['engravers'] :
    for TABLE in _get_with_fallback(ENGRAVER, 'tables', []) :
      engine.execute(CreateTable(TABLE))

  #@^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^@#
  # then, run engravers until there are no more changes
  LAST = -1
  CUR = conn.execute(SQL['counter']).fetchall()[0][0]
  
  while LAST != CUR :
    LAST = CUR
    for ENGRAVER in SQL['engravers'] :
      for elt in _get_with_fallback(ENGRAVER, 'inserter', []) :
        conn.execute(elt)
      for elt in _get_with_fallback(ENGRAVER, 'pointer', []) :
        conn.execute(elt)
      for elt in _get_with_fallback(ENGRAVER, 'updater', []) :
        conn.execute(elt)
    CUR = conn.execute(SQL['counter']).fetchall()[0][0]

  for row in conn.execute(select([Glyph_index.__table__])).fetchall() : print row
