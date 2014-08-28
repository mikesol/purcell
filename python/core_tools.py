from sqlalchemy import Table, MetaData, Column, Integer, Float, String
from sqlalchemy import select, cast, exists, distinct, case
from sqlalchemy import and_, or_
from sqlalchemy import func, event, text, DDL
from sqlalchemy.sql.selectable import Exists
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList
import string
import random
import math
import transitive_reduction

_GLOBAL_VERBOSE = False
_GLOBAL_INDEX = True

# i <3 you, randString!
def _randString(length=16, chars=string.letters):
  first = random.choice(string.letters[26:])
  return first+''.join([random.choice(chars) for i in range(length-1)])

def almost_equal(x,y) :
  return abs(x-y) < 10E-16

def sql_mod(x,y) :
  return case([(x==y, 0)], else_=(x+(x/y)) & y);

_metadata = MetaData()

class Fraction : pass
class Spanner : pass
class Box : pass
class Point : pass

###
class DummyConnection(object) :
  def __init__(self) :
    self.out = []
  def execute(self, thing) :
    self.out.append(str(thing))
  def __str__(self) :
    out = '\n'.join(self.out)
    return out
    
###

def make_table_generic(name, cols) :
  return Table(name, _metadata, *cols)

def make_table(name, tp, unique = False) :
  if tp == Fraction :
    # unique not possible
    return make_table_generic(name,
                     [Column('id', Integer, primary_key = True, index=_GLOBAL_INDEX),
                     Column('num', Integer),
                     Column('den', Integer)])
  if tp == Spanner :
    # unique not possible
    return make_table_generic(name,
                     [Column('id', Integer, primary_key = True, index=_GLOBAL_INDEX),
                     Column('left', Float),
                     Column('right', Float)])
  if tp == Box :
    # unique not possible
    return make_table_generic(name,
                     [Column('id', Integer, primary_key = True, index=_GLOBAL_INDEX),
                     Column('x', Float),
                     Column('y', Float),
                     Column('width', Float),
                     Column('height', Float),
                     ])
  if tp == Point :
    # unique not possible
    return make_table_generic(name,
                     [Column('id', Integer, primary_key = True, index=_GLOBAL_INDEX),
                     Column('x', Float),
                     Column('y', Float),
                     ])
  return Table(name, _metadata,
                     Column('id', Integer, primary_key = True, index=_GLOBAL_INDEX),
                     Column('val', tp, unique = unique))

##### tables

Used_ids = Table('used_ids', _metadata,
  Column('id', Integer, primary_key = True));

Log_table = Table('log_table', _metadata,
  Column('id', Integer, primary_key = True),
  Column('mom', Float),
  Column('msg', String))

Glyph_box = Table('glyph_box', _metadata,
  Column('name', String, primary_key = True),
  Column('unicode', String, primary_key = True),
  Column('x', Integer),
  Column('y', Integer),
  Column('width', Integer),
  Column('height', Integer)
  )

Unicode_to_glyph_index_map = Table('unicode_to_glyph_index_map', _metadata,
  Column('name', String, primary_key = True),
  Column('idx', Integer, primary_key = True),
  Column('unicode', String)
  )

String_box = Table('string_box', _metadata,
  Column('name', String, primary_key = True),
  Column('str', Integer, primary_key = True),
  Column('x', Integer),
  Column('y', Integer),
  Column('width', Integer),
  Column('height', Integer)
  )

def product(row) :
  return func.exp(func.sum(func.ln(1.0 * row)))

def product_i(row) :
  return cast(func.exp(func.sum(func.ln(1.0 * row))) + 0.3, Integer)

def den_overshoot(table, name) :
  distinct_dens =\
    select([distinct(table.c.den).label('den')]).cte(name=name+"_distinct")
  
  return select([product_i(distinct_dens.c.den).label('gcd')]).cte(name=name)


def bound_range(first, itTb, last=None, name = 'first_row', forward = True, extra_with = None) :
  with_condition = itTb.c.id == first
  if extra_with :
    with_condition = and_(extra_with, with_condition)
  table = select([itTb.c.id.label('elt')]).where(with_condition).cte(name = name, recursive = True)
  grow_me = table.alias(name = name+'_grow_me')
  going_to = itTb.c.val if forward else itTb.c.id
  coming_from = itTb.c.id if forward else itTb.c.val
  last_cond = (grow_me.c.elt == coming_from) if (last is None) \
          else and_(grow_me.c.elt == coming_from,
                    grow_me.c.elt != last)
  table = table.union_all(select([going_to.label('next')]).\
       where(last_cond))
  return table

class Ddl_holder(object) :
  def __init__(self, table, instruction) :
    self.table = table
    self.instruction = instruction

def gcd_table(table) :
  modulo = select([table.c.id,
                  table.c.id.label('raw_id'),
                  table.c.num,
                  table.c.den]).\
                    cte(name=table.name+"_gcd_first", recursive=True)

  modulo_a = modulo.alias(name = table.name+"_gcd_prev")

  modulo = modulo.union_all(
      select([
          (modulo_a.c.id + 1).label('id'),
          modulo_a.c.raw_id.label('raw_id'),
          func.mod(modulo_a.c.den, modulo_a.c.num).label('num'),
          modulo_a.c.num.label('den')
      ]).
          where(modulo_a.c.num > 0)
  )

  '''
  modulo = modulo.union_all(
      select([
          (modulo_a.c.id + 1).label('id'),
          modulo_a.c.raw_id.label('raw_id'),
          case([(modulo_a.c.den >= modulo_a.c.num, modulo_a.c.num)], else_= modulo_a.c.den).label('num'),
          case([(modulo_a.c.den >= modulo_a.c.num, modulo_a.c.den - modulo_a.c.num)], else_=modulo_a.c.num).label('den')
      ]).
          where(modulo_a.c.num > 0)
  )
  '''
  '''
  modulo = modulo.union_all(
      select([
          (modulo_a.c.id + 1).label('id'),
          modulo_a.c.raw_id.label('raw_id'),
          func.kludge_0(modulo_a.c.num, modulo_a.c.den).label('num'),
          func.kludge_1(modulo_a.c.num, modulo_a.c.den).label('den')
      ]).
          where(modulo_a.c.num > 0)
  )
  '''
  stmt = select([table.c.id.label('id'),
                 (table.c.num / modulo.c.den).label('num'),
                 (table.c.den / modulo.c.den).label('den')]).\
    select_from(
      table.\
        join(modulo, onclause=table.c.id == modulo.c.raw_id)).\
        where(modulo.c.num == 0)

  return stmt

def generate_sqlite_functions(conn) :
  def my_ln(in_val) :
    #print in_val, "LN"
    if in_val is None :
      return in_val
    return math.log(in_val)
  def my_exp(in_val) :
    #print in_val, "EXP"
    if in_val is None :
      return in_val
    return math.exp(in_val)
  def my_pow(in_val_0, in_val_1) :
    #print in_val_0, in_val_1, "POW"
    return in_val_0 ** in_val_1
  def my_mod(in_val_0, in_val_1) :
    #print in_val_0, in_val_1, "MOD"
    return in_val_0 % in_val_1
    #out = (in_val_0+(in_val_0/in_val_1)) & in_val_1
    #if in_val_0 == in_val_1 :
    #  out = 0
    #print in_val_0, in_val_1, out, "MOD", in_val_0 % in_val_1
    #return out
  def kludge_0(num, den) :
    print "K0", num, den, num if den >= num else den
    return den - num if den >= num else den
  def kludge_1(num, den) :
    print "K1", num, den, den if den >= num else num
    return den - num if den >= num else num

  conn.connection.create_function('ln', 1, my_ln)
  conn.connection.create_function('exp', 1, my_exp)
  conn.connection.create_function('pow', 2, my_pow)
  conn.connection.create_function('mod', 2, my_mod)
  conn.connection.create_function('kludge_0', 2, kludge_0)
  conn.connection.create_function('kludge_1', 2, kludge_1)

# debug before
# debug after

class InsertStmt(object) :
  def __init__(self) :
    self.stmts = []
    self.insert = None
  def generate_stmt(self, id) :
    self.stmts = []
    self.insert = None
    self._generate_stmt(id) 
  def get_stmt(self, id) :
    self.generate_stmt(id)
    return self.insert
  def register_stmt(self, stmt) :
    self.stmts.append(stmt)
  def register_insert(self, insert) :
    self.insert = insert
  def debug_before(self, conn, id, verbose = _GLOBAL_VERBOSE) :
    self.generate_stmt(id)
    for stmt in self.stmts :
      print "****", stmt.name, "****"
      if verbose :
        print select([stmt])
        print "&&&&&&&&&&&&&&&&&&&&&&&"
      try :
        for row in conn.execute(select([stmt])).fetchall() :
          print "  ", row
      except ResourceClosedError :
        print "no rows were returned from this statement"
    #if verbose :
    if True :
      print "HERE IS THE INSERT STMT"
      print self.insert
      print "))))))))))))))))) and the parameters"
      print self.insert.parameters
      print "&&&&&&&&&&&&&&&&&&&&&&&"
    #conn.execute(self.insert)
  def debug_after(self, conn, verbose = _GLOBAL_VERBOSE) :
    print "$$$ INSERTED into", self.insert.table.name, "$$$"

class DeleteStmt(object) :
  # probably should make dict_keys obligatory
  def __init__(self, table, where_clause_fn, dict_keys = 'id') :
    self.table = table
    self.where_clause_fn = where_clause_fn
    self.dict_keys = dict_keys.split(' ')
  def get_stmt(self, id) :
    where_clause = self.where_clause_fn(id)
    if isinstance(where_clause, Exists) :
      # uggghhh - giant, heinous kludge
      where_clause = where_clause.compile(compile_kwargs={"literal_binds": True})
      where_clause = str(where_clause).split('\n')
      for x in reversed(range(len(where_clause))) :
        # this is bad because it will inadvertently screw with other tables as well
        # probably need to bite the bullet and make better sqlite bindings
        if 'FROM' in where_clause[x] :
          where_clause[x] = where_clause[x].replace(", "+self.table.name, "")
          where_clause[x] = where_clause[x].replace(","+self.table.name, "")
          where_clause[x] = where_clause[x].replace(self.table.name, "")
          break
      where_clause = '\n'.join(where_clause)
    elif isinstance(where_clause, BinaryExpression) or isinstance(where_clause, BooleanClauseList)  :
      # hideous!!!
      where_clause = str(where_clause.compile(compile_kwargs={"literal_binds": True}))
    if where_clause :
      return self.table.delete().where(text(str(where_clause)))
    else :
      print "````` you may want to do some debugging here"
      print "````` a full delete is happening on", self.table.name
      return self.table.delete()
  def debug_before(self, id, conn, verbose = _GLOBAL_VERBOSE) :
    print "$$$", "BEFORE DELETE ON", self.table.name
    for row in conn.execute(select([self.table])) :
      print "    ", row
    print "&&&&& executing the DELETE statement below ON", self.table.name
    stmt = self.get_stmt(id)
    print stmt
    #conn.execute(stmt)
  def debug_after(self, id, conn, verbose = _GLOBAL_VERBOSE) :
    print "$$$", "AFTER DELETE ON", self.table.name
    for row in conn.execute(select([self.table])) :
      print "    ", row

class WhenStmt(object) :
  # probably should make dict_keys obligatory
  def __init__(self, clause_fn) :
    self.clause_fn = clause_fn
  def get_stmt(self, id) :
    clause = self.clause_fn(id)
    return clause

class EasyWhen(WhenStmt) :
  def __init__(self, *args) :
    def clause_fn(id) :
      referent = args[0]
      others = args[1:]
      stmt = select([referent.c.id])
      sf = referent
      for other in others :
        sf = sf.join(other, onclause=referent.c.id == other.c.id)
      return exists(stmt.select_from(sf))
    WhenStmt.__init__(self, clause_fn)



class DDL_unit(object) :
  def __init__(self, table, action, deletes = [], inserts = [], before = False, when_clause = None) :
    self.table = table
    self.action = action
    self.deletes = deletes
    self.inserts = inserts
    self.before = before
    self.when_clause = when_clause
  def as_ddl(self, LOG = False) :
    del_st = '\n'.join([str(delete.get_stmt('@ID@').compile(compile_kwargs={"literal_binds": True}))+";" for delete in self.deletes])
    inst_st = '\n'.join([str(insert.get_stmt('@ID@').compile(compile_kwargs={"literal_binds": True}))+";" for insert in self.inserts])
    when_st = ''
    if self.when_clause :
      when_st = 'WHEN ('+str(self.when_clause.get_stmt('@ID@').compile(compile_kwargs={"literal_binds": True}))+")"
    trigger = '''CREATE TRIGGER {2} {6} {1} ON {0} {7}
      BEGIN
          {3}
          {4}
          {5}
      END;
    '''
    tb_nm = '{0}_{1}'.format(self.table.name, string.lower(self.action))
    if (self.deletes) :
      tb_nm += '_del_'+'_'.join([stmt.table.name for stmt in self.deletes])
    if (self.inserts) :
      tb_nm += '_ins_'+'_'.join([stmt.insert.table.name for stmt in self.inserts])
    # arggg - we need to add a random tag to the trigger names in case multiple ddls operate on same tables
    tb_nm += '_'+_randString(4)
    log_st = "INSERT INTO log_table (mom, msg) VALUES (strftime('%%f','now'),'{0}');".format(tb_nm) if LOG else ''
    instr = trigger.format(self.table.name, self.action, tb_nm, log_st, del_st, inst_st, 'BEFORE' if self.before else 'AFTER', when_st)
    instr = instr.replace("'@ID@'", '{0}.id'.format('old' if self.action == 'DELETE' else 'new'))
    # ugggh...
    instr = instr.replace("@ID@", '{0}.id'.format('old' if self.action == 'DELETE' else 'new'))
    return Ddl_holder(self.table, instr)

class DDL_manager(object) :
  def __init__(self, ddls = []) :
    self.ddls = ddls
  def register_ddls(self, conn, LOG = False) :
    for ddl in self.ddls :
      holder = ddl.as_ddl(LOG)
      event.listen(holder.table, 'after_create', DDL(holder.instruction).\
               execute_if(dialect='sqlite'))
  def insert(self, conn, stmt, manual_ddl) :
    self.do_sql(conn, stmt, manual_ddl, 'INSERT')
  def update(self, conn, stmt, manual_ddl) :
    self.do_sql(conn, stmt, manual_ddl, 'UPDATE')
  def delete(self, conn, stmt, manual_ddl) :
    self.do_sql(conn, stmt, manual_ddl, 'DELETE')
  def do_sql(self, conn, stmt, manual_ddl, action) :
    if manual_ddl :
      ids = []
      if action == 'INSERT' :
        #print "==== dictdebug", stmt.__dict__
        if stmt.select != None :
          try :
            for row in conn.execute(stmt.select).fetchall() :
              ids.append(row[0])
          except ResourceClosedError :
            print "insert from select does not return any rows"
            #print "here is the statement"
            #print stmt
        else :
          ids = [stmt.parameters['id']]
      else :
        # uggghh
        for row in conn.execute(select([stmt.table]).where(stmt._whereclause)).fetchall() :
          ids.append(row[0])
      ids = list(set(ids))
      self.generic_ddl(True, conn, stmt.table, ids, action)
      print "/// NOW DOING MAIN", action, "ON", stmt.table.name, "WITH IDS", ids
      if hasattr(stmt, 'parameters') :
        if stmt.select == None :
          print "////// with explicit parameters", stmt.parameters
      conn.execute(stmt)
      if action == 'UPDATE' :
        ids = []
        for row in conn.execute(select([stmt.table]).where(stmt._whereclause)).fetchall() :
          ids.append(row[0])
      ids = list(set(ids))
      self.generic_ddl(False, conn, stmt.table, ids, action)
    else :
      conn.execute(stmt)
  def generic_ddl(self, before, conn, table, ids, action) :
    print "??????????? IDS", ids
    ddls = filter(lambda x : x.before == before, self.ddls)
    for ddl in ddls :
      if (ddl.table == table) and (ddl.action == action) :
        for id in ids :
          for delete in ddl.deletes :
            delete.debug_before(conn = conn, id = id)
            stmt = delete.get_stmt(id = id)
            print "--PERFORMING DELETE ON", stmt.table.name, "TRIGGERED By", action, "ON", table.name
            print stmt
            self.delete(conn, stmt, True)
            delete.debug_after(conn = conn, id = id)
          for insert in ddl.inserts :
            insert.debug_before(conn = conn, id = id)
            stmt = insert.get_stmt(id = id)
            print "--PERFORMING INSERT ON", stmt.table.name, "TRIGGERED By", action, "ON", table.name
            self.insert(conn, stmt, True)
            insert.debug_after(conn = conn)
  def transitively_reduce_ddl_list(self, filename='tmp.dot', safe_removals = []) :
    mapping = {}
    _x = 0
    edges = []
    edges_to_ddl = {}
    for ddl in self.ddls :
      table = ddl.table
      deletes = ddl.deletes
      inserts = ddl.inserts
      if not mapping.has_key(table) :
        mapping[table] = _x
        _x += 1
      for delete in deletes :
        if not mapping.has_key(delete.table) :
          mapping[delete.table] = _x
          _x += 1
        edges.append((mapping[table], mapping[delete.table]))
        if not edges_to_ddl.has_key(edges[-1]) :
          edges_to_ddl[edges[-1]] = []
        edges_to_ddl[edges[-1]].append(ddl)
      for insert in inserts :
        insert.generate_stmt("@ID@")
        if not mapping.has_key(insert.insert.table) :
          mapping[insert.insert.table] = _x
          _x += 1
        edges.append((mapping[table],mapping[insert.insert.table]))
        if not edges_to_ddl.has_key(edges[-1]) :
          edges_to_ddl[edges[-1]] = []
        edges_to_ddl[edges[-1]].append(ddl)
    edges = list(set(edges))
    for key, val in mapping.items() :
      mapping[val] = key
    ddls_to_nix = []
    edges_to_nix = []
    for removal in safe_removals :
      for edge in edges :
        if (mapping[edge[0]] == removal[0]) and (mapping[edge[1]] == removal[1]) :
          ddls_to_nix += edges_to_ddl[edge]
          edges_to_nix.append(edge)
    ddls_to_nix = list(set(ddls_to_nix))
    edges_to_nix = list(set(edges_to_nix))
    #print edges_to_nix, len(self.ddls)
    for ddl in ddls_to_nix :
      self.ddls.remove(ddl)
    #print len(self.ddls)
    for edge in edges_to_nix :
      edges.remove(edge)
    #for edge in edges :
    #  print edge, mapping[edge[0]], mapping[edge[1]]
    graph = transitive_reduction.Graph()
    graph.edges = list(edges)
    graph.nodes = list(set(sum(edges, ())))
    transitive_reduction.do_transitive_reduction_of_graph(graph)
    #for edge in graph.removed_edges :
    #  print edge, mapping[edge[0]], mapping[edge[1]]
    '''
    to_remove = []
    for ddl in self.ddls :
      table = ddl.table
      deletes = ddl.deletes
      inserts = ddl.inserts
      other_table = set([])
      for delete in deletes :
        other_table.add(delete.table)
      for insert in inserts :
        other_table.add(insert.insert.table)
      if len(other_table) > 1 :
        print "skpping", len(other_table), other_table
        continue
      E = (mapping[table], mapping[list(other_table)[0]])
      if E in graph.removed_edges :
        to_remove.append(ddl)
    for ddl in to_remove :
      self.ddls.remove(ddl)
    '''
    #"""
    out = 'digraph G{\n'
    for edge in edges :
      out += '  "{0}" -> "{1}";\n'.format(str(mapping[edge[0]]), str(mapping[edge[1]]))
    out += "}"
    FW = file(filename, 'w')
    FW.write(out)
    FW.close()
    #"""

def realize(to_realize, comp_t, prop) :
  #out = select([v.label(k) for k,v in to_realize.c.items()]).\
  # select_from(to_realize.outerjoin(comp_t, onclause = to_realize.c.id == comp_t.c.id)).\
  # where(comp_t.c[prop] == None).cte(name='realized_'+to_realize.name)

  #out = select([v.label(k) for k,v in to_realize.c.items()]).\
  #        except_(select([v.label(k) for k,v in comp_t.c.items()])).\
  #     cte(name='realized_'+to_realize.name)

  keys = comp_t.c.keys()
  out = select([to_realize.c[k].label(k) for k in keys]).\
          except_(select([comp_t.c[k] for k in keys])).\
       cte(name='realized_'+to_realize.name)
  return out

def simple_insert(table, fr) :
  return table.insert().from_select([key for key in table.c.keys()],
      select([val for val in fr.c.values()]))

def from_ft_20_6(foo) :
  return foo / 64.0

def sql_min_max(l, MAX=False) :
  fn = lambda x, y : or_(x <= y, x == None)
  if MAX :
    fn = lambda x, y : or_(x >= y, y == None)
  L = []
  for x in range(len(l)) :
    to_add = []
    for y in range(len(l)) :
      if x == y :
        continue
      to_add.append(fn(l[x], l[y]))
    L.append(and_(*to_add))
  OUT = [(L[x], l[x]) for x in range(len(L))]
  return case(OUT, else_ = None)

def safe_eq_comp(a,b) :
  if b == None :
    raise ValueError
  return a == b

def staff_spaceize(table, staff_symbol, staff_space) :
  return and_(table.c.id == staff_symbol.c.id, staff_symbol.c.val == staff_space.c.id)