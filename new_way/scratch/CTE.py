############ FIXME when sqlalchemy allows CTEs on non-SELECT statements ######
############ this can be deleted

def cte_with(self):
    '''CTE text for prepending to SELECT, UPDATE, CREATE, DELETEs'''
    if self.ctes:
        cte_text = self.get_cte_preamble(self.ctes_recursive) + " "
        cte_text += ", \n".join( [txt for txt in self.ctes.values()] )
        cte_text += "\n "
    else:
        cte_text = ""

    return cte_text

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Delete, Insert, Update

@compiles(Delete)
def cte_for_delete(delete, compiler, **kw):
    '''Do CTE WITH prefix'''
    # Call visit_delete first, or self.ctes will be empty.
    text = compiler.visit_delete(delete, **kw)
    with_prefix = cte_with(compiler)
    # This should be work with CTE aware delete, but not be necessary
    if with_prefix and text[:4] != 'WITH':
        text = with_prefix + text
    return text

@compiles(Insert)
def cte_for_insert(insert, compiler, **kw):
    '''Do CTE WITH prefix'''
    # Call visit_insert first, or self.ctes will be empty.
    text = compiler.visit_insert(insert, **kw)
    with_prefix = cte_with(compiler)
    # Oddness for inserts only, the WITH prefix gets into the RETURNING clause
    text = text.replace( with_prefix, '' )
    # This should be work with CTE aware insert, but not be necessary
    if with_prefix and text[:4] != 'WITH':
        text = with_prefix + text
    return text

@compiles(Update)
def cte_for_update(update, compiler, **kw):
    '''Do CTE WITH prefix'''
    # Call visit_update first, or self.ctes will be empty.
    text = compiler.visit_update(update, **kw)
    with_prefix = cte_with(compiler)
    # This should be work with CTE aware update, but not be necessary
    if with_prefix and text[:4] != 'WITH':
        text = with_prefix + text
    return text
