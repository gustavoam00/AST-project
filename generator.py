import random
import string
from dataclasses import dataclass, field
from typing import List, Optional, Union, Dict
from config import SEED
import copy

# random.seed(SEED)
INSIDE_INDEX = False
VIRTUAL = {
    "types": ["rtree", "fts4", "dbstat"],
    "dbstat": [{"name": "TEXT"}, {"path": "TEXT"}, {"pageno": "INTEGER"}, {"pagetype": "TEXT"}, {"ncell": "INTEGER"}, {"payload": "INTEGER"}, {"unused": "INTEGER"}, {"mx_payload": "INTEGER"}, {"pgoffset": "INTEGER"}, {"pgsize": "INTEGER"}, {"schema": "TEXT"}],
    "rtree": [{"id": "INTEGER", "constraint": "PRIMARY KEY"}, {"minX": "REAL"}, {"maxX": "REAL"}, {"minY": "REAL"}, {"maxY": "REAL"}],
    "fts4": [{"title": "TEXT"}, {"body": "TEXT"}]
}
SQL_TYPES = ["INTEGER", "TEXT", "REAL"]
SQL_CONSTRAINTS = ["PRIMARY KEY", "UNIQUE", "NOT NULL", "CHECK", "DEFAULT"]
VALUES = { # put interesting values to test here
    "INTEGER": [0, 1, -1, 
                2**31-1, -2**31, 
                2**63-1, -2**63,
                999999999999999999999999999999999999999999999999999999999999999999999999,
                42, 1337,
                0x7FFFFFFF, 0x80000000,
                ],
    "TEXT": ["''", "' '", "'a'", "'abc'", "'" + " "*50 + "'", 
             #"' OR 1=1; --'", "'\x00\x01\x02'", 
             #"DROP TABLE test;", #"A"*10000,
             "'quoted'", "'NULL'",
             ],
    "REAL": [0.0, -0.0, 1.0, -1.0,
             3.14159, 2.71828,
             #float('inf'), float('-inf'), float('nan'),
             1e-10, 1e10, 1e308, -1e308,
             ],
}
TIME = {
    "TIMES": ["'now'",
            "'2025-01-01 12:00:00'",
            "'2020-06-15 08:45:00'",
            "'2000-01-01 00:00:00'",
            "'2030-12-31 23:59:59'",
            "'1700000000'",
    ],
    "TIME_MODS": [
            "'+1 day'", "'-2 days'", "'+3 hours'", "'-90 minutes'",
            "'start of month'", "'start of year'", "'weekday 0'",
            "'+1 month'", "'-1 year'",
            "'utc'", "'localtime'",
    ],
    "TIME_FORMATS": [
        "'%Y-%m-%d'", "'%H:%M:%S'", "'%s'", "'%w'", "'%Y %W'", "'%j'", "'%Y-%m-%d %H:%M'"
    ],
    "DATES": ['datetime', 'date', 'time', 'julianday', 'strftime'],
    "CURRENT": ['current_date', 'current_time', 'current_timestamp']
}
CALLABLE_VALUES = {
    "INTEGER": lambda: random.randint(-10000, 10000),
    "TEXT": lambda: ("'" + random_name(prefix = "v", length=5) + "'"),
    "REAL": lambda: random.uniform(-1e5, 1e5),
}
OPS = {
    "INTEGER": ["=", "!=", ">", "<", ">=", "<="],
    "TEXT": ["=", "!="],
    "REAL": ["=", "!=", ">", "<", ">=", "<="],
    "TYPELESS": ["=", "!="],
}

def random_name(prefix: str = "x", length: int = 5) -> str:
    """
    Generate a random string, used for naming

    Args:
        prefix (str, optional): Prefix for naming purposes. Defaults to "x".
        length (int, optional): Length of string. Defaults to 5.

    Returns:
        str: the random string
    """
    suffix = ''.join(random.choices(string.ascii_lowercase, k=length)) #add string.digits?
    return f"{prefix}_{suffix}"

def random_type() -> str:
    """
    Returns a random data type

    Returns:
        str: the dtype
    """
    return random.choice(SQL_TYPES)

def random_value(dtype:str, null_chance:float = 0.05, callable_chance:float = 0.9) -> str:
    """
    Returns a random value given the dtype, could be either interesting values or completely random.

    Args:
        dtype (str): type of value
        null_chance (float, optional): chance of null. Defaults to 0.05.
        callable_chance (float, optional): chance of completely random. Defaults to 0.9.

    Returns:
        str: _description_
    """
    if flip(null_chance) or dtype == "NULL":
        return "NULL"
    
    dtype = random_type() if dtype == "TYPELESS" or dtype is None else dtype
    return str(CALLABLE_VALUES[dtype]()) if flip(callable_chance) else str(random.choice(VALUES[dtype]))
    
def flip(weight:float = 0.5) -> bool:  
    """
    Flipping weighted coin

    Args:
        weight (float, optional): Likelihood. Defaults to 0.5.

    Returns:
        bool: success
    """
    return random.random() < weight

def random_chars(k):
    return "'" + ''.join(random.choices(string.ascii_letters + ' ', k=k)) + "'"

def nonzero_random_value(dtype: str):
    val = "0"
    while val == "0":
        val = random_value(dtype, null_chance=0)
    return val
    
def apply_random_formula(expr: str, dtype: str) -> tuple[str, str]:
    """
    Applies a random SQLite expression-compatible transformation to the given expression,
    returning the new SQL expression and its resulting type.

    Args:
        expr (str): (column name, literal, any expression)
        dtype (str): (TEXT, INTEGER, REAL)

    Returns:
        tuple[str, str]: (transformed expression, resulting SQL type)
    """
    transformations = [
        (f"{expr} < {random_value(dtype, null_chance=0)}", "INTEGER"),
        (f"{expr} <= {random_value(dtype, null_chance=0)}", "INTEGER"),
        (f"{expr} > {random_value(dtype, null_chance=0)}", "INTEGER"),
        (f"{expr} >= {random_value(dtype, null_chance=0)}", "INTEGER"),
        (f"{expr} = {random_value(dtype, null_chance=0)}", "INTEGER"),
        (f"{expr} != {random_value(dtype, null_chance=0)}", "INTEGER"),
        (f"{random_value(dtype, null_chance=0)} > {expr}", "INTEGER"),
        (f"{random_value(dtype, null_chance=0)} >= {expr}", "INTEGER"),
        (f"{random_value(dtype, null_chance=0)} < {expr}", "INTEGER"),
        (f"{random_value(dtype, null_chance=0)} <= {expr}", "INTEGER"),
        (f"{random_value(dtype, null_chance=0)} = {expr}", "INTEGER"),
        (f"{random_value(dtype, null_chance=0)} != {expr}", "INTEGER"),
        (f"{expr} IS NULL", "INTEGER"),
        (f"{expr} IS NOT NULL", "INTEGER"),
        (f"nullif({expr}, {expr})", "NULL"),
        (f"nullif({expr}, {random_value(dtype, null_chance=0)})", dtype),
        (f"TYPEOF({expr})", "TEXT"),
        (f"LIKELY({expr})", dtype),
        (f"UNLIKELY({expr})", dtype),
        (f"LIKELIHOOD({expr}, 0.5)", dtype),
    ]

    if dtype == "TEXT":
        transformations.extend([
            (f"LOWER({expr})", "TEXT"),
            (f"UPPER({expr})", "TEXT"),
            (f"HEX({expr})", "TEXT"),
            (f"QUOTE({expr})", "TEXT"),
            (f"TRIM({expr}, {random_chars(1)})", "TEXT"),
            (f"LTRIM({expr}, {random_chars(1)})", "TEXT"),
            (f"RTRIM({expr}, {random_chars(1)})", "TEXT"),
            (f"REPLACE({expr}, {random_chars(2)}, {random_chars(2)})", "TEXT"),
            (f"SUBSTR({expr}, {random.randint(1, 5)}, {random.randint(1, 5)})", "TEXT"),
            (f"{expr} || {random_chars(3)}", "TEXT"),
            (f"{random_chars(3)} || {expr}", "TEXT"),
            (f"INSTR({expr}, {random_chars(2)})", "INTEGER"),
            (f"LENGTH({expr})", "INTEGER"),
            (f"{expr} LIKE {random_chars(4)}", "INTEGER"),
            (f"{expr} GLOB {random_chars(4)}", "INTEGER"),
            (f"UNICODE({expr})", "INTEGER"),
            (f"PRINTF('%10s', {expr})", "TEXT"),
            (f"PRINTF('%-10s', {expr})", "TEXT"),
            (f"PRINTF('%.3s', {expr})", "TEXT"),
            (f"{expr} + {random_value('INTEGER', null_chance=0)}", "INTEGER"),
            (f"{random_value('INTEGER', null_chance=0)} + {expr}", "INTEGER"),
            (f"{expr} - {random_value('INTEGER', null_chance=0)}", "INTEGER"),
            (f"{random_value('INTEGER', null_chance=0)} - {expr}", "INTEGER"),
            (f"{expr} * {random_value('INTEGER', null_chance=0)}", "INTEGER"),
            (f"{random_value('INTEGER', null_chance=0)} * {expr}", "INTEGER"),
            (f"{expr} / {nonzero_random_value(dtype)}", "REAL"),
        ])
    elif dtype in ("INTEGER", "REAL"): #it seems that for the most part TEXT can alwys be converted to Numerical, so all of these could be used?
        transformations.extend([
            (f"ABS({expr})", dtype),
            (f"ROUND({expr})", dtype),
            (f"ROUND({expr}, {random.randint(0, 3)})", dtype),
            (f"COALESCE(NULL, {expr})", dtype),
            (f"COALESCE(NULL, {expr}, {random_value(dtype, null_chance=0.0)})", dtype),
            (f"COALESCE(NULL, NULL, {expr})", dtype),
            (f"COALESCE({expr}, {random_value(dtype, null_chance=0.0)})", dtype),
            (f"IFNULL(NULL, {expr})", dtype),
            (f"IFNULL({expr}, {random_value(dtype, null_chance=0.0)})", dtype),
            (f"- ({expr})", dtype),
            (f"+ ({expr})", dtype),
            (f"PRINTF('{'%.2f' if dtype == 'REAL' else '%d'}', {expr})", "TEXT"),
            (f"PRINTF('%x', {expr})", "TEXT"),
            (f"PRINTF('%o', {expr})", "TEXT"),
            (f"PRINTF('%c', {expr})", "TEXT"),
            (f"PRINTF('%.6e', {expr})", "TEXT"),
            (f"PRINTF('%.1g', {expr})", "TEXT"),
            (f"PRINTF('%.0f%%', {expr})", "TEXT"),
            (f"{expr} + {random_value(dtype, null_chance=0)}", dtype),
            (f"{random_value(dtype, null_chance=0)} + {expr}", dtype),
            (f"{expr} - {random_value(dtype, null_chance=0)}", dtype),
            (f"{random_value(dtype, null_chance=0)} - {expr}", dtype),
            (f"{expr} * {random_value(dtype, null_chance=0)}", dtype),
            (f"{random_value(dtype, null_chance=0)} * {expr}", dtype),
            (f"{expr} / {nonzero_random_value(dtype)}", "REAL"),
            (f"{expr} / NULLIF({0},{1})", "REAL"),
            (f"{expr} / NULLIF({0},{0})", "NULL"),
        ])
    elif dtype == "TYPELESS":
        return expr, dtype

    return random.choice(transformations)

def apply_random_aggregate_function(expr: str, dtype: str) -> tuple[str, str]:
    """
    Applies a random SQLite aggregate function to the given expression,
    returning the new SQL expression and its resulting type.

    Args:
        expr (str): SQL expression
        dtype (str): Data type of the expression (TEXT, INTEGER, REAL)

    Returns:
        tuple[str, str]: (aggregate expression, result dtype)
    """

    aggregates = [
        (f"COUNT({expr})", "INTEGER"),
        (f"COUNT(DISTINCT {expr})", "INTEGER"),
        (f"MAX({expr})", dtype),
        (f"MIN({expr})", dtype)
    ]

    if dtype in ["INTEGER", "REAL"]:
        aggregates += [
            (f"AVG({expr})", "REAL"),
            (f"SUM({expr})", dtype),
            (f"TOTAL({expr})", "REAL")
        ]
    
    if dtype == "TEXT":
        aggregates += [
            (f"GROUP_CONCAT({expr})", "TEXT"),
            (f"GROUP_CONCAT({expr}, ', ')", "TEXT")
        ]

    return random.choice(aggregates)


def apply_random_cast(expr: str, dtype: str) -> tuple[str, str]:
    """
    Applies a random valid CAST to the given expression, based on its current dtype.

    Args:
        expr (str): (column name, literal, or expression).
        dtype (str): (TEXT, INTEGER, REAL).

    Returns:
        tuple[str, str]: (transformed expression using CAST, new resulting dtype)
    """

    valid_casts = {
        "TEXT":    ["INTEGER", "REAL"],
        "INTEGER": ["TEXT", "REAL"],
        "REAL":    ["TEXT", "INTEGER"],
    }

    target = random.choice(valid_casts.get(dtype, ["TEXT",]))

    cast_expr = f"CAST({expr} AS {target})"
    return cast_expr, target

def random_expression(dtype:str="TYPELESS") -> str:
    """
    Generates a ranomd standalone expression.
    
    Args:
        dtype (str, optional): Possible type of expression . Defaults to "TYPELESS".

    Returns:
        str: the expression
    """
    if dtype == "INTEGER":
        options = [
            "RANDOM()",
            "TRUE",
            "FALSE",
            f"UNICODE({random_chars(1)})",
            ":myparam",
        ]
    elif dtype == "TEXT":
        options = [
            f"CHAR({', '.join(str(random.randint(0, 1114111)) for _ in range(random.randint(0, 4)))})",
            ":myparam",
        ]
    else:
        return random_value(dtype, null_chance=0)
    return random.choice(options)

#----------------------------------------------------------------------------------------------------------------------------------------------------#

class SQLNode:
    def sql(self) -> str:
        return ""
        #raise NotImplementedError


#----------------------------------------------------------------------------------------------------------------------------------------------------#

@dataclass
class Predicate(SQLNode):
    """
    Any predicate: Nullcheck, Comparison, InList, Between, and Like
    """
    def sql(self) -> str:
        return ""
    
    @staticmethod
    def random(table: "Table", sub_allow: bool = True, param_prob: Dict[str, float] = None) -> "Predicate":
        prob = {}
        if param_prob is not None:
            prob.update(param_prob)
            
        col = random.choice(table.columns)
        
        predicate_classes = [NullCheck, Comparison, InList, Between]
        if sub_allow:
            predicate_classes += [Exists]
        
        # if col.dtype == "INTEGER" or col.dtype == "REAL":
        #     predicate_classes.append(Between)
        if col.dtype == "TEXT":
            predicate_classes.append(Like)
        
        cls = random.choice(predicate_classes)
        if cls == Exists:
            return cls.random(table, param_prob=prob)
        else:
            return cls.random(col, table_name=table.name, param_prob=prob)
    
@dataclass
class Comparison(Predicate):
    '''
    column OP value
    '''
    column: "Column"
    operator: str
    value: str
    table_name: str
    
    def sql(self) -> str:
        ret = f"{self.table_name}." if self.table_name else ""
        return ret + f"{self.column.name} {self.operator} {self.value}"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "Comparison":
        prob = {"comp_nullc": 0.05, "comp_callc": 0.9}
        if param_prob is not None:
            prob.update(param_prob)
        dtype = col.dtype
        op = random.choice(OPS[dtype])
        val = Expression.random(dtype=dtype, no_cols=True, param_prob=prob).sql()
        # val = random_value(dtype, prob["comp_nullc"], prob["comp_callc"])
        return Comparison(col, op, val, table_name)
    
    def mutate(self) -> "Comparison":
        comparison = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "change_col", "change_op", "change_val"])

        if mutation_type == "rename":
            comparison.table_name = random_name("mut_comp")

        elif mutation_type == "change_col":
            comparison.column = comparison.column.mutate()

        elif mutation_type == "change_op":
            comparison.operator = random.choice(OPS[comparison.column.dtype])

        elif mutation_type == "change_val":
            comparison.value = random_value(comparison.column.dtype)

        return comparison

@dataclass
class Between(Predicate):
    '''
    column BETWEEN low AND high
    '''
    column: "Column"
    lower: str
    upper: str
    table_name: str

    def sql(self) -> str:
        ret = f"{self.table_name}." if self.table_name else ""
        return ret + f"{self.column.name} BETWEEN {self.lower} AND {self.upper}"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "Between":
        prob = {"bet_nullc" : 0, "bet_callc" : 0.9}
        if param_prob is not None:
            prob.update(param_prob)
        
        v1 = random_value(col.dtype, prob["bet_nullc"], prob["bet_callc"])
        v2 = random_value(col.dtype, prob["bet_nullc"], prob["bet_callc"])
        low, high = sorted([v1, v2])
        return Between(col, low, high, table_name)
    
    def mutate(self) -> "Between":
        between = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "change_col", "change_lower", "change_upper"])

        if mutation_type == "rename":
            between.table_name = random_name("mut_betw")

        elif mutation_type == "change_col":
            between.column = between.column.mutate()

        elif mutation_type == "change_lower":
            between.lower = random_value(between.column.dtype)

        elif mutation_type == "change_upper":
            between.upper = random_value(between.column.dtype)

        return between

@dataclass
class Like(Predicate):
    '''
    column LIKE val
    '''
    column: "Column"
    val: str
    table_name: str

    def sql(self) -> str:
        ret = f"{self.table_name}." if self.table_name else ""
        return ret + f"{self.column.name} LIKE {self.val}"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "Like":
        prob = {"like_nullc" : 0.05, "like_callc" : 0.9}
        if param_prob is not None:
            prob.update(param_prob)
        val = Like.generate_like_pattern(random_value("TEXT", prob["like_nullc"], prob["like_callc"]))
        return Like(col, val, table_name)
    
    @staticmethod
    def generate_like_pattern(base: str) -> str:
        '''
        Helper function to create patterns for LIKE, could be more complex if we wanted
        '''
        if not base:
            return random.choice(['%', '_', ''])
        
        base = base.replace("'", "")

        patterns = []
        patterns.append(f"{base}")
        patterns.append(f"{base}")
        
        patterns.append(f"%{base}")
        patterns.append(f"{base}%")
        patterns.append(f"%{base}%")
        patterns.append(f"_{base}")
        patterns.append(f"{base}_")
        
        if len(base) > 2:
            idx = random.randint(1, len(base) - 2)
            pattern = base[:idx] + '%' + base[idx:]
            patterns.append(pattern)

        if len(base) > 1:
            idx = random.randint(0, len(base) - 1)
            pattern = base[:idx] + '_' + base[idx + 1:]
            patterns.append(pattern)

        return "'" + random.choice(patterns) + "'"
    
    def mutate(self) -> "Like":
        like = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "change_col", "change_val"])

        if mutation_type == "rename":
            like.table_name = random_name("mut_like")

        elif mutation_type == "change_col":
            like.column = like.column.mutate()

        elif mutation_type == "change_val":
            like.val = Like.generate_like_pattern(random_value("TEXT"))

        return like
    
@dataclass
class InList(Predicate):
    '''
    column in (v1, v2, v3, ...)
    '''
    column: "Column"
    values: List[str]
    table_name: str = ""

    def sql(self) -> str:
        value_list = ', '.join(self.values)
        ret = f"{self.table_name}." if self.table_name else ""
        return ret + f"{self.column.name} IN ({value_list})"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "InList":
        prob = {"inli_nullc" : 0.05, "inli_callc" : 0.9}
        if param_prob is not None:
            prob.update(param_prob)
        prob.update({
            "null_p":prob["inli_nullc"],
            "call_p":prob["inli_callc"]
        })
        count = random.randint(2, 5)
        values = [Expression.random(dtype=col.dtype, no_cols=True, param_prob=prob).sql() for _ in range(count)]
        # values = [random_value(col.dtype, prob["inli_nullc"], prob["inli_callc"]) for _ in range(count)]
        return InList(col, values, table_name)
    
    def mutate(self) -> "InList":
        inlist = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "add_value", "remove_value", "change_col"])

        if mutation_type == "rename":
            inlist.table_name = random_name("mut_inli")

        elif mutation_type == "add_value":
            inlist.values.append(random_value(inlist.column.dtype))

        elif mutation_type == "remove_value" and inlist.values:
            inlist.values.pop(random.randint(0, len(inlist.values) - 1))

        elif mutation_type == "change_col":
            inlist.column = inlist.column.mutate()

        return inlist
    
@dataclass
class Exists(Predicate):
    '''
    EXISTS (SELECT 1 FROM ... WHERE ...)
    '''
    select: "Select"

    def sql(self) -> str:
        return f"EXISTS ({self.select.sql()})"

    @staticmethod
    def random(table: "Table", param_prob: Dict[str, float] = None) -> "Exists":
        prob = {"where_ex_p" : 1, "grp_ex_p" : 0, "ord_ex_p" : 0, "*_ex_p":0, "cols_ex_p":0, "one_ex_p":1, "omit_ex_p":0, "lit_ex_p":1}
        if param_prob is not None:
            prob.update(param_prob)
        prob.update({
            "where_p" : prob["where_ex_p"], 
            "grp_p" : prob["grp_ex_p"], 
            "ord_p" : prob["ord_ex_p"], 
            "*_p":prob["*_ex_p"], 
            "cols_p":prob["cols_ex_p"], 
            "one_p":prob["one_ex_p"], 
            "omit_p":prob["omit_ex_p"], 
            "lit_p":prob["lit_ex_p"]
        })
        select = Select.random(table, sample=1, param_prob=prob)
        return Exists(select)
    
    def mutate(self) -> "Exists":
        exists = copy.deepcopy(self)
        exists.select = exists.select.mutate()

        return exists

@dataclass
class NullCheck(Predicate):
    '''
    column IS/IS NOT NULL
    '''
    column: "Column"
    check: bool
    table_name: str

    def sql(self) -> str:
        ret = f"{self.table_name}." if self.table_name else ""
        if self.check:
            return ret + f"{self.column.name} IS NULL"
        else:
            return ret + f"{self.column.name} IS NOT NULL"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "NullCheck":
        prob = {"nullc" : 0.5}
        if param_prob is not None:
            prob.update(param_prob)
        check = flip(prob["nullc"])
        return NullCheck(col, check, table_name)
    
    def mutate(self) -> "NullCheck":
        null_check = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "change_col", "toggle_nullc"])

        if mutation_type == "rename":
            null_check.table_name = random_name("mut_nullc")

        elif mutation_type == "change_col":
            null_check.column = null_check.column.mutate()

        elif mutation_type == "toggle_nullc":
            null_check.check = not null_check.check

        return null_check

#----------------------------------------------------------------------------------------------------------------------------------------------------#

@dataclass
class Expression(SQLNode):
    """
    Expression is anything that computes a value.
    
    """
    def sql(self) -> str:
        return ""
    
    @staticmethod
    def random(table: Optional["Table"]=None, column:Optional["Column"]=None, dtype:str=None, no_cols:bool=False, agg:bool = False, param_prob: Dict[str, float]=None):
        prob = {"nocol_p":0.5, "cole_p": 0.8, "lit_p":0.9, "case_p":0.05, "time_p": 0.05}
        if param_prob is not None:
            prob.update(param_prob)

            
        if agg:
            if flip(prob["lit_p"] / (prob["lit_p"] + prob["cole_p"])):
                return Literal.random(dtype=dtype, agg=True, param_prob=prob)
            else:
                return ColumnExpression.random(table, column=column, agg=True, param_prob=prob)

        elif no_cols or flip(prob["nocol_p"]):
            if (dtype and dtype != "TEXT") or flip(prob["lit_p"] / (prob["lit_p"] + prob["time_p"])):
                return Literal.random(dtype=dtype, param_prob=prob)
            else:
                return Time.random(param_prob=prob)

        else:
            if flip(prob["cole_p"] / (prob["cole_p"] + prob["case_p"])):
                return ColumnExpression.random(table, column=column, param_prob=prob)
            else:
                return Case.random(table, column=column, dtype=dtype, param_prob=prob)

@dataclass
class Literal(Expression):
    """
    Any Literal such as explicit values, funcitons, or formulas
    """
    value: str
    dtype: str
    
    def sql(self) -> str:
        return self.value
    
    @staticmethod
    def random(dtype:str = None, agg:bool = False, param_prob: Dict[str, float] = None) -> "Literal":
        prob = {"null_p":0.01, "call_p":0.9, "one_p":0.01, "std_p": 0.75, "form_p":0.3, "cast_p":0.05, "agg2_p": 0.5, "rexp_p":0.01, "alias_p":0.01}
        if param_prob is not None:
            prob.update(param_prob)
            
        if flip(prob["one_p"]):
            return Literal(value="1", dtype="INTEGER")
        
        if flip(prob["rexp_p"]):
            value = random_expression(dtype=dtype)
        else:
            value = random_value(dtype=dtype, null_chance=prob["null_p"], callable_chance=prob["call_p"])
        current_dtype = dtype
        
        if flip(prob["std_p"]):
            return Literal(value=value, dtype=current_dtype)
        
        for _ in range(random.randint(1,3)):
            if flip(prob["form_p"]):
                value, current_dtype = apply_random_formula(value, current_dtype)
            if flip(prob["cast_p"]):
                value, current_dtype = apply_random_cast(value, current_dtype)
        
        if agg and flip(prob["agg2_p"]):
            value, current_dtype = apply_random_aggregate_function(value, current_dtype) #aggregrate cannot be nested
            
        if dtype and current_dtype != dtype:
            value = f"CAST({value} AS {dtype})"
        
        return Literal(value, current_dtype)
    
    def mutate(self) -> "Literal":
        literal = copy.deepcopy(self)
        mutation_type = random.choice(["apply_formula", "apply_cast", "apply_agg"])

        current_dtype = literal.dtype

        if mutation_type == "apply_formula":
            literal.value, literal.dtype = apply_random_formula(literal.value, current_dtype)

        elif mutation_type == "apply_cast":
            literal.value, literal.dtype = apply_random_cast(literal.value, current_dtype)

        elif mutation_type == "apply_agg":
            literal.value, literal.dtype = apply_random_aggregate_function(literal.value, current_dtype)

        return literal
    
@dataclass
class Time(Expression):
    """
    Time expressions
    """
    value:str
    
    def sql(self) -> str:
        return self.value
    
    @staticmethod
    def random(param_prob: Dict[str, float]=None) -> "Time":
        prob = {}
        if param_prob is not None:
            prob.update(param_prob)
        
        options = [
            f"{random.choice(TIME['DATES'])}({random.choice(TIME['TIMES'])})",
            f"{random.choice(TIME['DATES'])}({random.choice(TIME['TIMES'])}, {random.choice(TIME['TIME_MODS'])})",
            f"strftime({random.choice(TIME['TIME_FORMATS'])}, {random.choice(TIME['TIMES'])}, {random.choice(TIME['TIME_MODS'])})",
            f"{random.choice(TIME['CURRENT'])}"
        ]
        return Time(value=random.choice(options))
    
    def mutate(self) -> "Time":
        time = copy.deepcopy(self)
        options = [
            f"{random.choice(TIME['DATES'])}({random.choice(TIME['TIMES'])})",
            f"{random.choice(TIME['DATES'])}({random.choice(TIME['TIMES'])}, {random.choice(TIME['TIME_MODS'])})",
            f"strftime({random.choice(TIME['TIME_FORMATS'])}, {random.choice(TIME['TIMES'])}, {random.choice(TIME['TIME_MODS'])})",
            f"{random.choice(TIME['CURRENT'])}"
        ]
        time.value = random.choice(options)

        return time

@dataclass
class ColumnExpression(Expression):
    """
    Expressions that explicitly include a column
    """
    value: str
    table: "Table"
    
    def sql(self) -> str:   
        return self.value

    @staticmethod
    def random(table: "Table", column:Optional["Column"]=None, agg:bool = False, param_prob: Dict[str, float] = None) -> "ColumnExpression":
        prob = {"std_p": 0.4, "form_p": 0.4, "cast_p":0.2,"alias_p":0.01}
        if param_prob is not None:
            prob.update(param_prob)

        if column is None and table.columns:
            column = random.choice(table.columns)
        current_dtype = column.dtype
        value = f"{table.name}.{column.name}"
            
        if flip(prob["std_p"]):
            return ColumnExpression(value=value, table=table)
        
        for _ in range(random.randint(1,3)):
            if flip(prob["form_p"]):
                value, current_dtype = apply_random_formula(value, current_dtype)
            if flip(prob["cast_p"]):
                value, current_dtype = apply_random_cast(value, current_dtype)
            
        if agg:
            value, current_dtype = apply_random_aggregate_function(value, current_dtype)
        
        return ColumnExpression(value=value, table=table)
    
    def mutate(self) -> "ColumnExpression":
        col_expr = copy.deepcopy(self)
        mutation_type = random.choice(["change_val", "apply_formula", "apply_cast", "apply_agg", "change_tbl"])

        if mutation_type == "change_val" and col_expr.table.columns:
            column = random.choice(col_expr.table.columns)
            col_expr.value = f"{table.name}.{column.name}"

        elif mutation_type == "apply_formula" and col_expr.table.columns:
            current_dtype = random.choice(col_expr.table.columns).dtype 
            col_expr.value, _ = apply_random_formula(col_expr.value, current_dtype) 

        elif mutation_type == "apply_cast" and col_expr.table.columns:
            current_dtype = random.choice(col_expr.table.columns).dtype 
            col_expr.value, _ = apply_random_cast(col_expr.value, current_dtype)

        elif mutation_type == "apply_agg" and col_expr.table.columns:
            current_dtype = random.choice(col_expr.table.columns).dtype 
            col_expr.value, _ = apply_random_aggregate_function(col_expr.value, current_dtype)

        elif mutation_type == "change_tbl":
            col_expr.table = col_expr.table.mutate()

        return col_expr
        
@dataclass
class Case(Expression):
    """
    Case Expressions
    """
    conditions:List[str]
    values:List[str]
    col:str
    else_:str
    dtype: str
    
    def sql(self) -> str:
        query = f"CASE "
        if self.col:
            query += f"{self.col} "
        
        for conditon, value in zip(self.conditions, self.values):
            query += f"WHEN {conditon} THEN {value} "
            
        if self.else_:
            query += f"ELSE {self.else_} "
            
        query += "END"    
        return query

    @staticmethod
    def random(table: "Table", column:Optional["Column"]=None, dtype:Optional[str]=None, param_prob: Dict[str, float] = None) -> "Case":
        prob = {"case_col_p" : 0.5}
        if param_prob is not None:
            prob.update(param_prob)
            
        if column:
            col = f"{table.name}.{column.name}"
        else:
            column = random.choice(table.columns)
            col = "" if flip(prob["case_col_p"]) else f"{table.name}.{column.name}"
        col_dtype = column.dtype
        
        if not dtype:
            dtype = random_type()
            
        num_cases = random.randint(1,5)
        conditions = []
        values = []
        for _ in range(num_cases):
            if col:
                conditions.append(random_value(col_dtype))
            else:
                conditions.append(Comparison.random(column, param_prob=prob).sql())
                
            values.append(random_value(dtype))
        
        else_ = "" if flip() else random_value(dtype)
        return Case(conditions, values, col, else_, col_dtype)
    
    def mutate(self) -> "Case":
        case = copy.deepcopy(self)
        mutation_type = random.choice(["change_else", "change_val", "apply_formula"])

        if mutation_type == "change_else":
            case.else_ = random_value(case.dtype) if flip() else "" 
        
        elif mutation_type == "change_val":
            case.values = [random_value(case.dtype) for _ in case.values]
        
        elif mutation_type == "apply_formula":
            for i in range(len(case.values)):
                case.values[i], _ = apply_random_formula(case.values[i], case.dtype)

        return case
        
#----------------------------------------------------------------------------------------------------------------------------------------------------#   

@dataclass
class Where(SQLNode):
    '''
    WHERE Predicate/InSubquery/WHERE
    '''
    def sql(self) -> str:
        return ""

    @staticmethod
    def random(table: "Table", max_depth: int = 1, no_sub: bool = False,other_tables: List["Table"] = None, param_prob: Dict[str, float] = None) -> "Where":
        prob = {"pred_p":1.0, "depth_p":0.7, "sub_p":0.05, "where_p":0.05}
        if param_prob is not None:
            prob.update(param_prob)
            
        other_tables = other_tables or []

        if max_depth <= 0 or flip(prob["depth_p"]):
            if no_sub:
                return Predicate.random(table, sub_allow=False, param_prob=prob) # Index does not accept subqueries
            elif other_tables and flip(prob["sub_p"]):
                return InSubquery.random(table, other_tables, param_prob=prob) #{"where_p":prob["where_p"]})
            elif flip(prob["pred_p"]):
                return Predicate.random(table, param_prob=prob)
            else:
                return Predicate.random(table, sub_allow=False, param_prob=prob)
        else:
            left = Where.random(table, max_depth - 1, no_sub = no_sub, param_prob=prob)
            right = Where.random(table, max_depth - 1, no_sub = no_sub, param_prob=prob)
            op = random.choice(["AND", "OR"])
            return BooleanExpr(left, right, op)
        
    def mutate(self) -> "Where":
        return None

@dataclass
class InSubquery(Where):
    '''
    WHERE col IN (SELECT other_col FROM other_table WHERE ...)
    '''
    column: "Column" 
    subquery: "Select"
    table_name: str

    def sql(self) -> str: 
        return f"{self.table_name}.{self.column.name} IN ({self.subquery.sql()})" #is this missing the WHERE at the beginning of the string?

    @staticmethod
    def random(table: "Table", other_tables: List["Table"],  param_prob: Dict[str, float] = None) -> "InSubquery":
        prob = {"where_p":0.05, }
        if param_prob is not None:
            prob.update(param_prob)
            
        column = random.choice(table.columns)
        other_table = random.choice(other_tables)

        matching_columns = [col for col in other_table.columns if col.dtype == column.dtype]
        if not matching_columns:
            sub_col = random.choice(other_table.columns)
        else:
            sub_col = random.choice(matching_columns)
            
        where_clause = Where.random(other_table, max_depth=1, param_prob=prob) if flip(prob["where_p"]) else None
        subquery = Select(
            expressions = [f"{other_table.name}.{sub_col.name}",],
            columns=[sub_col,],
            from_clause=other_table,
            where=where_clause
        )

        return InSubquery(column=column, subquery=subquery, table_name=table.name)
    
    def mutate(self) -> "InSubquery":
        in_subquery = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "change_col", "change_subquery"])

        if mutation_type == "rename":
            in_subquery.table_name = random_name("mut_insub")

        elif mutation_type == "change_col":
            in_subquery.column = in_subquery.column.mutate()

        elif mutation_type == "change_subquery":
            in_subquery.subquery = in_subquery.subquery.mutate()

        return in_subquery

@dataclass
class BooleanExpr(Where):
    '''
    a < b
    '''
    left: Where
    right: Where
    operator: str 

    def sql(self) -> str:
        return f"({self.left.sql()} {self.operator} {self.right.sql()})"
    
    def mutate(self) -> "BooleanExpr":
        bool_exp = copy.deepcopy(self)
        mutation_type = random.choice(["change_op", "swap", "modify_left", "modify_right"])

        if mutation_type == "change_op":
            bool_exp.operator = random.choice(["<", ">", "=", "<=", ">=", "<>", "IS", "IS NOT"])

        elif mutation_type == "swap":
            bool_exp.left, bool_exp.right = bool_exp.right, bool_exp.left

        elif mutation_type == "modify_left":
            bool_exp.left = bool_exp.left.mutate()

        elif mutation_type == "modify_right":
            bool_exp.right = bool_exp.right.mutate()

        return bool_exp

#----------------------------------------------------------------------------------------------------------------------------------------------------#

@dataclass
class Column:
    '''
    name dtype primary_key nullable unique check default
    '''
    name: str
    dtype: str
    nullable: bool = True
    primary_key: bool = False
    notnull: bool = False
    unique: bool = False #unique and primary key are likely to cause an error bc of randomly generating same value, integer worst
    check: Optional[Predicate] = None #checking enforces a predicate that is basically never gonna be satisfied, causing crashes
    default: Optional[str] = None
    from_table: str = ""

    def sql(self) -> str:
        col = [self.name, ]
        if self.dtype != "TYPELESS":
            col.append(self.dtype)
        else:
            pass #apparently putting random strings as user defined types is allowed
        
        # if self.primary_key:
        #     col.append("PRIMARY KEY")
        if self.notnull:
            col.append("NOT NULL")
        if self.unique:
            col.append("UNIQUE")
        if self.check:
            col.append(f"CHECK({self.check.sql()})")
        if self.default:
            col.append(f"DEFAULT {self.default}")
        return " ".join(col)

    @staticmethod
    def random(name: Optional[str] = None, param_prob:Dict[str, float] = None) -> "Column":
        prob = {"pk_p":0.0, "unq_p":0.001, "dft_p":0.2, "nnl_p":0.01, "cck_p":0.3, "typeless_p":0.1}
        if param_prob is not None:
            prob.update(param_prob)
            
        if flip(prob["typeless_p"]):
            dtype = "TYPELESS"
        else:
            dtype = random_type()
            
        name = name or random_name("col")
        name = dtype[0].lower() + name
        primary_key = flip(prob["pk_p"]) and dtype != "TYPELESS"
        unique = not primary_key and flip(prob["unq_p"])

        check = None 
        # if flip(prob["cck_p"]):
        #     temp_table = Table(name="fake", columns=[])
        #     fake_col = Column(name=name, dtype=dtype)
        #     temp_table.columns.append(fake_col)
        #     check = Predicate.random(temp_table, rand_tbl=0)

        default = random_value(dtype, null_chance=0) if flip(prob["dft_p"]) and not primary_key and not unique else None
        notnull = default and flip(prob["nnl_p"])
        nullable = not primary_key and not notnull
        
        return Column(name, dtype, nullable, primary_key, notnull, unique, check, default)
    
    def mutate(self) -> "Column":
        column = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "change_dtype", "toggle_nullable", "toggle_primary_key", "toggle_unique", "toggle_check", "change_default"])

        if mutation_type == "rename":
            column.name = random_name("col")

        elif mutation_type == "change_dtype":
            column.dtype = random_type()

        elif mutation_type == "toggle_nullable":
            column.nullable = not column.nullable

        elif mutation_type == "toggle_primary_key":
            column.primary_key = not column.primary_key

        elif mutation_type == "toggle_unique":
            column.unique = not column.unique

        elif mutation_type == "toggle_check":
            if column.check is not None:
                column.check = None
            else:
                column.check = Comparison.random(column) 

        elif mutation_type == "change_default":
            if column.default is None:
                column.default = random_value(column.dtype)
            else:
                column.default = None

        return column

@dataclass
class Table(SQLNode):
    '''
    CREATE TABLE name (columns)
    '''
    name: str
    columns: List[Column]
    viewed = False

    def sql(self) -> str:
        column_defs = ", ".join([col.sql() for col in self.columns])
        return f"CREATE TABLE {self.name} ({column_defs})"

    def get_col_names(self) -> List[str]:
        return [col.name for col in self.columns]
    
    @staticmethod
    def random(name: Optional[str] = None, min_cols: int = 2, max_cols: int = 6) -> "Table":
        name = name or random_name("tbl")
        num_cols = random.randint(min_cols, max_cols)

        columns = []
        for i in range(num_cols):
            # primary key only for first column and try at least 1 non unique col
            prob = {"pk_p":0.1 if i==0 else 0, "unq_p": 0 if i==0 or i==1 else 0.05}
            col = Column.random(param_prob=prob)
            columns.append(col)
        return Table(name, columns)
    
    def mutate(self) -> "Table":
        table = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "add_col", "remove_col", "mutate_col"])

        if mutation_type == "rename":
            table.name += f"_{random.choice(['x', '1', 'tmp'])}"

        elif mutation_type == "add_col":
            new_col = Column.random()
            table.columns.append(new_col)

        elif mutation_type == "remove_col" and len(table.columns) > 1:
            idx = random.randrange(len(table.columns))
            del table.columns[idx]

        elif mutation_type == "mutate_col" and table.columns:
            col = random.choice(table.columns)
            col.mutate()

        return table
    
@dataclass
class AlterTable(Table):
    '''
    ALTER TABLE name RENAME old_col TO new_col
    ALTER TABLE name ADD COLUMN new_col
    ALTER TABLE old_name RENAME TO name
    '''
    name: str
    table: Table
    old_name: str = ""
    new_col: Optional[Column] = None
    old_col_name: str = ""
    columns: List[Column]
    idx: Optional[int] = 0

    def sql(self) -> str:
        if self.old_col_name:
            return f"ALTER TABLE {self.name} RENAME {self.old_col_name} TO {self.new_col.name}"
        elif self.new_col:
            return f"ALTER TABLE {self.name} ADD COLUMN {self.new_col.sql()}"
        else:
            return f"ALTER TABLE {self.old_name} RENAME TO {self.name}"
        
    @staticmethod
    def random(table: "Table") -> "AlterTable":
        if table.viewed:  # modifying tables breaks views
            return None
        
        fn = random.choice([AlterTable.random_add, AlterTable.random_col_rename, AlterTable.random_tbl_rename])
        return fn(table)
        
    @staticmethod
    def random_add(table: "Table") -> "AlterTable":
        new_col = Column.random(param_prob={"unq_p":0.0})
        modified_cols = copy.deepcopy(table.columns)
        modified_cols.append(new_col)
        return AlterTable(name=table.name, table=table, new_col=new_col, columns=modified_cols)
    
    def confirm_add(self):
        self.table.columns = self.columns
    
    @staticmethod
    def random_col_rename(table: "Table") -> "AlterTable":
        if table.viewed:
            return None
        mod_cols = copy.deepcopy(table.columns)
        idx = random.randrange(len(mod_cols))
        mod_col = mod_cols[idx]
        old_col_name = mod_col.name
        mod_col.name = random_name("col")
        return AlterTable(name=table.name, table=table, new_col=mod_col, old_col_name=old_col_name, columns=mod_cols, idx=idx)
    
    def confirm_rename(self):
        self.table.columns[self.idx] = self.new_col

    
    @staticmethod
    def random_tbl_rename(table: "Table") -> "AlterTable":
        if table.viewed:
            return None
        return AlterTable(name=random_name("atbl"), table=table, old_name=table.name, columns=table.columns)
    
    def mutate(self) -> "AlterTable":
        alter_table = copy.deepcopy(self)
        return AlterTable.random(alter_table)
        
#----------------------------------------------------------------------------------------------------------------------------------------------------#

@dataclass
class Insert(SQLNode):
    """
    INSERT [OR...] (cols) VAlUES values
    """
    table: Table
    columns: List[Column]
    values: List[List[str]]
    default: bool
    full: bool
    conflict_action: Optional[str] = None

    def sql(self) -> str:
        query = "INSERT "
        if self.conflict_action:
            query+= f"OR {self.conflict_action} "
            
        cols = ", ".join([c.name for c in self.columns])
        vals_list = []
        for row in self.values:
            vals = ", ".join(row)
            vals_list.append(f"({vals})")
        all_vals = ", ".join(vals_list)
        
        query += f"INTO {self.table.name} "
        if not self.full:
            query += f"({cols}) "
            
        if self.default:
           query += f"DEFAULT VALUES"
        else:
            query += f"VALUES {all_vals}"
        
        return query

    @staticmethod
    def random(table: "Table", non_unique: bool = False, param_prob:Dict[str, float] = None) -> "Insert":
        prob = {"dft_p":0.2, "full_p":0.2, "conf_p":0.2, "null_p":0.05, "call_p":0.9}
        if param_prob is not None:
            prob.update(param_prob)
        
        candidate_cols = [col for col in table.columns if not non_unique or not (col.unique or col.primary_key)]
        if not candidate_cols and non_unique:
            return None
        
        default = flip(prob["dft_p"]) and all(col.nullable for col in table.columns)
        conflict_action = random.choice(["ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"]) if flip(prob["conf_p"]) else None
        full = flip(prob["full_p"]) and not non_unique
        
        if default:
           return Insert(table=table, columns=[], values=[], conflict_action=conflict_action, default=True, full=True)
        
        cols = []
        num_rows = random.randint(1,5)
        vals = [[] for _ in range(num_rows)]
        
        if full:
            sample_cols = table.columns
        else:
            num_cols = random.randint(1, len(candidate_cols))
            sample_cols = random.sample(candidate_cols, num_cols)

        for col in sample_cols:
            cols.append(col)
            for i in range(num_rows):
                if not col.nullable or col.unique or col.primary_key:
                    prob["null_p"] = 0 
                    prob["call_p"] = 1 
                    prob["rexp_p"] = 0
                    prob["std_p"] = 1
                value = Expression.random(dtype=col.dtype, no_cols=True, param_prob=prob).sql()
                # value = random_value(col.dtype, null_chance=prob["null_p"], callable_chance=prob["call_p"])
                vals[i].append(value)

        return Insert(table=table, columns=cols, values=vals, conflict_action=conflict_action, default=default, full=full)
    
    def mutate(self) -> "Insert":
        insert = copy.deepcopy(self)
        mutation_type = random.choice(["change_val", "change_tbl", "toggle_full", "toggle_default"])

        if mutation_type == "change_tbl":
            insert.table = insert.table.mutate()

        elif mutation_type == "change_val" and insert.values and insert.columns:
            row_idx = random.randint(0, len(insert.values) - 1)
            col_idx = random.randint(0, len(insert.columns) - 1)
            insert.values[row_idx][col_idx] = random_value(insert.columns[col_idx].dtype)

        elif mutation_type == "toggle_full":
            insert.full = not insert.full

        elif mutation_type == "toggle_default":
            insert.default = not insert.default

        return insert
    
@dataclass
class Update(SQLNode):
    """
    UPDATE table SET columns = values WHERE ... 
    """
    table: Table
    columns: List[Column]
    values: List[str]
    where: Optional[Where] = None

    def sql(self) -> str:
        query = f"UPDATE {self.table.name} SET "
        assignments = []
        for column, value in zip(self.columns, self.values):
            assignments.append(f"{column.name} = {value}")
        query += ", ".join(assignments)
        if self.where:
            query += f" WHERE {self.where.sql()}"
        return query

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Update":
        prob = {"where_p":0.75}
        if param_prob is not None:
            prob.update(param_prob)
            
        cols = []
        vals = []
        
        candidate_cols = [col for col in table.columns if not (col.unique or col.primary_key)]
        if not candidate_cols: #no non-unique columns, update likely to crash
            candidate_cols = col
        
        num_cols = random.randint(1, len(candidate_cols))
        sample_cols = random.sample(candidate_cols, num_cols)
        
        for col in sample_cols: 
            cols.append(col)
            prob["null_p"] = 0 if col.notnull else 0.05
            value = Expression.random(dtype=col.dtype, no_cols=True, param_prob=prob).sql()
            # value = random_value(col.dtype, null_chance=prob["null_p"])
            vals.append(value)

        where = Where.random(table, param_prob=prob) if flip(prob["where_p"]) else None
        return Update(table=table, columns=cols, values=vals, where=where)
    
    
    def mutate(self) -> "Update":
        update = copy.deepcopy(self)
        mutation_type = random.choice(["change_val", "change_tbl", "change_where", "toggle_where"])

        if mutation_type == "change_tbl":
            update.table = update.table.mutate()

        elif mutation_type == "change_val" and update.columns:
            col_idx = random.randint(0, len(update.columns) - 1)
            update.values[col_idx] = random_value(update.columns[col_idx].dtype)

        elif mutation_type == "change_where":
            update.where = update.where.mutate() if update.where else None

        elif mutation_type == "toggle_where":
            update.where = Where.random(update.table) if not update.where else None

        return update
    
@dataclass   
class Delete(SQLNode):
    """
    DELETE FROM table WHERE ...
    """
    table: Table
    where: Optional[Where] = None

    def sql(self) -> str:
        query = f"DELETE FROM {self.table.name}"
        if self.where:
            query += f" WHERE {self.where.sql()}"
        return query

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Delete":
        prob = {"where_p":0.95}
        if param_prob is not None:
            prob.update(param_prob)
            
        where = Where.random(table, param_prob=prob) if flip(prob["where_p"]) else None
        return Delete(table=table, where=where)
    
    def mutate(self) -> "Delete":
        delete = copy.deepcopy(self)
        mutation_type = random.choice(["change_tbl", "change_where", "toggle_where"])

        if mutation_type == "change_tbl":
            delete.table = delete.table.mutate()

        elif mutation_type == "change_where":
            delete.where = delete.where.mutate() if delete.where else None

        elif mutation_type == "toggle_where":
            delete.where = Where.random(delete.table) if not delete.where else None

        return delete
    
@dataclass
class Replace(SQLNode):
    """
    REPLACE INTO table (cols) VALUES values
    """
    table: Table
    columns: List[Column]
    values: List[List[str]]
    default: bool
    full: bool

    def sql(self) -> str:
        query = f"REPLACE INTO {self.table.name} "
            
        cols = ", ".join([c.name for c in self.columns])
        vals_list = []
        for row in self.values:
            vals = ", ".join(row)
            vals_list.append(f"({vals})")
        all_vals = ", ".join(vals_list)
        
        if not self.full:
            query += f"({cols}) "
            
        if self.default:
           query += f"DEFAULT VALUES"
        else:
            query += f"VALUES {all_vals}"
        
        return query

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Replace":
        prob = {"dft_p":0.2, "full_p":0.2, "null_p":0.05, "call_p":0.9}
        if param_prob is not None:
            prob.update(param_prob)
            
        default = flip(prob["dft_p"]) and all(col.nullable for col in table.columns)
        full = flip(prob["full_p"])
        
        if default:
           return Replace(table=table, columns=[], values=[], default=True, full=True)
        
        cols = []
        num_rows = random.randint(1,5)
        vals = [[] for _ in range(num_rows)]
        
        if full:
            sample_cols = table.columns
        else:
            num_cols = random.randint(1, len(table.columns))
            sample_cols = random.sample(table.columns, num_cols)

        for col in sample_cols:
            cols.append(col)
            for i in range(num_rows):
                if not col.nullable or col.unique or col.primary_key:
                    prob["null_p"] = 0 
                    prob["call_p"] = 1 
                    prob["rexp_p"] = 0
                    prob["std_p"] = 1
                value = Expression.random(dtype=col.dtype, no_cols=True, param_prob=prob).sql()
                # value = random_value(col.dtype, null_chance=prob["null_p"], callable_chance=prob["call_p"])
                vals[i].append(value)

        return Replace(table=table, columns=cols, values=vals, default=default, full=full)
    
    def mutate(self) -> "Replace":
        replace = copy.deepcopy(self)
        mutation_type = random.choice(["change_val", "change_tbl", "toggle_full", "toggle_default"])

        if mutation_type == "change_tbl":
            replace.table = replace.table.mutate()

        elif mutation_type == "change_val" and replace.values and replace.columns:
            row_idx = random.randint(0, len(replace.values) - 1)
            col_idx = random.randint(0, len(replace.columns) - 1)
            replace.values[row_idx][col_idx] = random_value(replace.columns[col_idx].dtype)

        elif mutation_type == "toggle_full":
            replace.full = not replace.full

        elif mutation_type == "toggle_default":
            replace.default = not replace.default

        return replace
    
#----------------------------------------------------------------------------------------------------------------------------------------------------#

@dataclass
class Join(SQLNode):
    '''
    table1 INNER/LEFT/CROSS JOIN table2 
    '''
    left_table: Table
    right_table: Table
    left_column: Column
    right_column: Column
    join_type: str
    alias: bool

    def sql(self) -> str:
        if self.alias:
            if self.join_type == "CROSS":
                return f"{self.left_table.name} AS a CROSS JOIN {self.right_table.name} AS b"
            
            return (
                f"{self.left_table.name} AS a {self.join_type} JOIN {self.right_table.name} AS b "
                f"ON a.{self.left_column.name} = b.{self.right_column.name}"
            )
        else :
            if self.join_type == "CROSS":
                return f"{self.left_table.name} CROSS JOIN {self.right_table.name}"
            
            return (
                f"{self.left_table.name} {self.join_type} JOIN {self.right_table.name} "
                f"ON {self.left_table.name}.{self.left_column.name} = {self.right_table.name}.{self.right_column.name}"
            )

    @staticmethod
    def random(left: "Table", right: "Table", join_type: str = None) -> "Join":
        left_cols = [c for c in left.columns if c.dtype in {"INTEGER", "TEXT"}]
        right_cols = [c for c in right.columns if c.dtype in {"INTEGER", "TEXT"}]

        alias = left.name == right.name
            
        if not left_cols or not right_cols:
            left_col = random.choice(left.columns)
            right_col = random.choice(right.columns)
        else:
            left_col = random.choice(left_cols)
            right_col = random.choice(right_cols)

        if not join_type:
            join_type = random.choice(["INNER", "LEFT", "CROSS"])
            
        return Join(
            left_table=left,
            right_table=right,
            left_column=left_col,
            right_column=right_col,
            join_type=join_type,
            alias=alias
        )
    
    def mutate(self) -> "Join":
        join = copy.deepcopy(self)
        mutation_type = random.choice(["join_type", "columns", "left_table", "right_table", "toggle_alias"])

        if mutation_type == "join_type":
            join.join_type = random.choice(["INNER", "LEFT", "CROSS"])

        elif mutation_type == "columns":
            left_cols = [c for c in join.left_table.columns if c.dtype in {"INTEGER", "TEXT"}]
            right_cols = [c for c in join.right_table.columns if c.dtype in {"INTEGER", "TEXT"}]
            
            if left_cols and right_cols:
                join.left_column = random.choice(left_cols)
                join.right_column = random.choice(right_cols)

        elif mutation_type == "left_table":
            join.left_table = join.left_table.mutate()
            join.alias = join.left_table.name == join.right_table.name

        elif mutation_type == "right_table":
            join.right_table = join.right_table.mutate()
            join.alias = join.left_table.name == join.right_table.name

        elif mutation_type == "toggle_alias":
            join.alias = not join.alias

        return join

@dataclass
class Select(SQLNode):
    """
    SELECT expressions [...];
    """
    expressions: List[str]
    from_clause: Union[Table, Join]
    asterisk: bool = False
    omit:bool = False
    date:bool = False
    where: Optional[Where] = None
    group_by: Optional[List[Column]] = None
    order_by: Optional[List[Column]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    columns: Optional[List[Column]] = None
    table_name: Optional[str] = None

    def sql(self) -> str: 
        base = "SELECT "
        if self.asterisk:
            base += "*"
        else:
            base += f"{', '.join(self.expressions)}"
            
        if self.omit:
            return base
        
        base += f" FROM {self.from_clause.sql() if isinstance(self.from_clause, Join) else self.from_clause.name}"
        
        if self.where:
            base += f" WHERE {self.where.sql()}"
        if self.group_by:
            group_names = [f"{self.table_name}.{c.name}" for c in self.group_by]
            base += f" GROUP BY {', '.join(group_names)}"
        if self.order_by:
            order_names = [f"{self.table_name}.{c.name}" for c in self.order_by]
            base += f" ORDER BY {', '.join(order_names)}"
        if self.limit:
            base += f" LIMIT {self.limit}"
            if self.offset:
                base+= f" OFFSET {self.offset}"
                
        return base

    @staticmethod
    def random(table: Table, sample: int = None, other_tables: list[Table] = None, param_cols: List[Column] = None, param_prob:Dict[str, float] = None) -> "Select":
        prob = {"where_p":0.9, "grp_p":0.3, "ord_p":0.3, "join_p":0.3, "lmt_p":0.2, "case_p":0.05, "offst_p":0.5, "*_p":0.2, "omit_p":0.1, "one_p":0.05, "date_p":0.1, "cols_p":0.5, "agg_p":0.1, "count_p":0.3, "alias_p": 0.05}
        if param_prob is not None:
            prob.update(param_prob)
        
        if param_cols:
            cols = param_cols
        else:
            cols = table.columns
        
        if flip(prob["*_p"]): # option 1: * (ALL)
            asterisk = True
            omit = False
            selected_cols = cols
            expressions = [] 
            
        elif flip(prob["cols_p"]): #option 2: only cols
            asterisk = False
            omit = False
            num = sample if sample else random.randint(1, len(cols))
            selected_cols = random.sample(cols, num)
            expressions = [f"{table.name}.{c.name}" for c in selected_cols] 
            
        elif flip(prob["omit_p"]): #option 3: only literals because omit (SELECT _ ;)
            asterisk = False
            omit = True
            num = sample if sample else random.randint(1, 10)
            selected_cols = None
            expressions = [Expression.random(table, no_cols=True, param_prob=prob).sql() for _ in range(num)]
        
        else: #option 4 any expression
            asterisk = False
            omit = False
            num = sample if sample else random.randint(1, 10)
            selected_cols = []
            if flip(prob["agg_p"]):
                expressions = [Expression.random(table, agg=True, param_prob=prob).sql() for _ in range(num)]
                if flip(prob["count_p"]):
                    expressions.append("COUNT(*)")
            else:
                expressions = [Expression.random(table, param_prob=prob).sql() for _ in range(num)]
        
        for i in range(len(expressions)):
            if flip(prob["alias_p"]):
                expressions[i] = f"{expressions[i]} AS {random_name(prefix='alias')}"

            
        where = Where.random(table, param_prob=prob, other_tables=other_tables) if flip(prob["where_p"]) else None
        group_by = random.sample(selected_cols, k=1) if selected_cols and flip(prob["grp_p"]) else None
        order_by = random.sample(selected_cols, k=1) if selected_cols and flip(prob["ord_p"]) else None
        limit = random.randint(1,20) if flip(prob["lmt_p"]) else None
        offset = random.randint(1,20) if flip(prob["offst_p"]) and limit else None
        
        if other_tables and flip(prob["join_p"]):
            left = table
            right = random.choice(other_tables)
            from_clause = Join.random(left, right)
            if left.name == right.name:
                asterisk = True
                omit = False
                group_by = None
                order_by = None
                where = None
            
        else:
            from_clause = table
            
        return Select(
            expressions=expressions,
            asterisk=asterisk,
            omit=omit,
            from_clause=from_clause,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
            columns=selected_cols,
            table_name = table.name
        )
    
    def mutate(self) -> "Select":
        select = copy.deepcopy(self)
        mutation_type = random.choice([
            "expressions", "toggle_where", "change_where", "group_by", "order_by", "limit", "offset",
            "from_clause", "toggle_asterisk", "toggle_omit"
        ])

        if mutation_type == "expressions" and not select.asterisk and not select.omit and select.columns:
            col = random.choice(select.columns)
            expr_type = random.choice(["simple", "agg", "literal"])
            if expr_type == "simple":
                select.expressions.append(f"{col.name}")
            elif expr_type == "agg":
                select.expressions.append(f"MAX({col.name})")
            else:
                select.expressions.append(str(random.randint(1, 100)))

        elif mutation_type == "toggle_where" and select.columns:
            select.where = Where.random(select.from_clause if isinstance(select.from_clause, Table) else select.from_clause.left_table) if not select.where else None

        elif mutation_type == "change_where":
            select.where = select.where.mutate() if select.where else None

        elif mutation_type == "group_by" and select.columns:
            select.group_by = random.sample(select.columns, k=min(1, len(select.columns)))

        elif mutation_type == "order_by" and select.columns:
            select.order_by = random.sample(select.columns, k=min(1, len(select.columns)))

        elif mutation_type == "limit":
            select.limit = random.randint(1, 100)

        elif mutation_type == "offset" and select.limit:
            select.offset = random.randint(1, 50)

        elif mutation_type == "from_clause":
            select.from_clause = select.from_clause.mutate()

        elif mutation_type == "toggle_asterisk":
            select.asterisk = not select.asterisk

        elif mutation_type == "toggle_omit":
            select.omit = not select.omit

        return select
        
@dataclass
class With(SQLNode):
    '''
    WITH name AS (
        SELECT ... FROM ... WHERE ...
    ) 
    SELECT ... FROM ... WHERE ...
    '''
    names: List[str]
    querys: List[Select]
    main_query: SQLNode
    recursive: bool = False

    def sql(self) -> str:
        full_query = f"WITH {'RECURSIVE' if self.recursive else ''} "
        
        temp = []
        for i in range(len(self.names)):
            temp.append(f"{self.names[i]} AS ({self.querys[i].sql()})")
        full_query += ", ".join(temp)
        if self.main_query:
            return f"{full_query} {self.main_query.sql()}"
        return full_query
    
    @staticmethod
    def random(table: Table, param_prob: Dict[str,float] = None) -> "With":
        prob = {"rec_p":0.5, "one_with_p":0, "*_with_p":1, "select_p":0.95, }
        if param_prob is not None:
            prob.update(param_prob)
        prob.update({
            "one_p":prob["one_with_p"],
            "*_p":prob["*_with_p"],
        })
        recursive = flip(prob["rec_p"])
        num = random.randint(1,3)
        with_names = [random_name("with") for _ in range(num)]
        with_tables = [Table(with_name, table.columns) for with_name in with_names]
        inner_selects = [Select.random(random.choice([table] + with_tables[:i]), param_prob=prob) for i in range(num)]
        
        idx = random.randint(0, num-1)
        cols = inner_selects[idx].columns
        w_table = with_tables[idx]
        if flip(prob["select_p"]) or isinstance(table, View):
            main_query = Select.random(w_table, param_cols=cols, param_prob=prob)
        else:
            fn = random.choice([Delete.random, Insert.random, Replace.random, Update.random])
            main_query = fn(table, param_prob=prob)#using the w_table inside these is pretty complicated, so the with here is useless but correct syntax
        
        return With(names=with_names, querys=inner_selects, main_query=main_query, recursive=recursive)
    
    def mutate(self) -> "With":
        w = copy.deepcopy(self)
        mutation_type = random.choice([
            "rename", "change_inner", "change_main", "toggle_recursive", "add_cte", "remove_cte"
        ])

        if mutation_type == "rename":
            idx = random.randrange(len(w.names))
            w.names[idx] += f"_{random.choice(['x', 'tmp', 'v2'])}"
            
        elif mutation_type == "change_inner" and w.querys:
            idx = random.randrange(0, len(w.querys))
            w.querys[idx] = w.querys[idx].mutate()

        elif mutation_type == "change_main":
            w.main_query = w.main_query.mutate() # Select, Delete, Insert, Replace, Update

        elif mutation_type == "toggle_recursive":
            w.recursive = not w.recursive

        elif mutation_type == "add_cte" and w.querys:
            new_name = random_name("with")
            new_query = random.choice(w.querys).mutate()
            w.names.append(new_name)
            w.querys.append(new_query)

        elif mutation_type == "remove_cte" and len(w.names) > 1:
            idx = random.randrange(len(w.names))
            del w.names[idx]
            del w.querys[idx]

        return w
    
#----------------------------------------------------------------------------------------------------------------------------------------------------#

@dataclass
class View(Table):
    '''
    CREATE VIEW name AS SELECT ... 
    '''
    name: str
    select: Select
    columns: List[Column]
    temp: bool

    def sql(self) -> str:
        return f"CREATE {'TEMP' if self.temp else ''} VIEW {self.name} AS {self.select.sql()}"
    
    @staticmethod
    def random(table: Table, other_tables: List[Table] = None, param_prob: Dict[str,float] = None) -> "View":
        prob = {"tmp_p":0.01, "one_view_p":0, "*_p":0.5, "cols_view_p":1, "alias_view_p":0, "rexp_view_p":0}
        if param_prob is not None:
            prob.update(param_prob)
        prob.update({
            "one_p":prob["one_view_p"],
            "cols_p":prob["cols_view_p"],
            "alias_p":prob["alias_view_p"],
            "rexp_p":prob["rexp_view_p"]
        })
            
        temp = flip(prob["tmp_p"])
        view_name = random_name("view")
        
        select = Select.random(table, other_tables=other_tables, sample=random.randint(1, len(table.columns)), param_prob=prob)

        return View(name=view_name, columns=select.columns, select=select, temp=temp)
    
    def mutate(self) -> "View":
        view = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "toggle_temp", "change_select", "change_col"])

        if mutation_type == "rename":
            view.name += f"_{random.choice(['v2', 'x', 'tmp'])}"

        elif mutation_type == "toggle_temp":
            view.temp = not view.temp

        elif mutation_type == "change_select" and view.select:
            view.select = view.select.mutate()

        elif mutation_type == "change_col" and view.columns:
            col = random.choice(view.columns)
            col.mutate()

        return view

@dataclass
class VirtualTable(Table):
    """
    CREATE VIRTUAL TABLE table USING ... [(cols)]
    """
    name: str
    columns: List[Column]
    vtype: str
    viewed: bool = True 

    def sql(self) -> str:
        if self.vtype == "dbstat":
            return f"CREATE VIRTUAL TABLE {self.name} USING {self.vtype}"
        else:
            return f"CREATE VIRTUAL TABLE {self.name} USING {self.vtype}({', '.join([c.name for c in self.columns])})"
    
    def random() -> "VirtualTable":
        vtype = random.choice(VIRTUAL["types"])
        col_names = VIRTUAL[vtype]
        if vtype == "fts4": 
            cols = random.randint(2, 6)
            columns = [Column(name=random_name("fts_col"), dtype="TEXT") for _ in range(cols)]
        else:
            columns = []
            for c in col_names:
                keys = list(c)
                if len(keys) == 1:
                    columns.append(Column(name=keys[0], dtype=c[keys[0]]))
                else:
                    columns.append(Column(name=keys[0], dtype=c[keys[0]], primary_key=True))
        return VirtualTable(name=random_name(vtype), columns=columns, vtype=vtype)
    
    def mutate(self) -> "VirtualTable":
        vt = copy.deepcopy(self)
        mutation_type = random.choice([
            "rename", "change_col", "toggle_viewed"
        ])

        if mutation_type == "rename":
            vt.name += f"_{random.choice(['v2', 'alt', 'x'])}"

        elif mutation_type == "change_col" and vt.vtype == "fts4" and vt.columns:
            col = random.choice(vt.columns)
            col.mutate()

        elif mutation_type == "toggle_viewed":
            vt.viewed = not vt.viewed

        return vt


@dataclass
class Index(SQLNode):
    '''
    CREATE (UNIQUE) INDEX name ON table (idx_columns) WHERE ...
    '''
    name: str
    table: str
    columns: List[str]
    unique: bool
    where: Optional[Where] = None

    def sql(self) -> str:
        query = ["CREATE"]
        if self.unique:
            query.append("UNIQUE")
        query.append(f"INDEX {self.name} ON {self.table} ({', '.join(self.columns)})")
        if self.where:
            query.append(f"WHERE {self.where.sql()}")
        return " ".join(query)

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Index":
        if isinstance(table, View):
            return None # cannot index on View
        
        prob = {"uniq_p":0.001, "where_p": 0.4, "rexp_index_p":0, "time_index_p":0, "std_index_p": 1}
        if param_prob is not None:
            prob.update(param_prob)
        prob.update({
            "rexp_p": prob["rexp_index_p"],
            "time_p": prob["time_index_p"],
            "std_p": prob["std_index_p"],
        })
          
        name = random_name("idx")
        col_names = table.get_col_names()
        num_cols = random.randint(1, min(len(col_names), 3))
        cols = random.sample(col_names, num_cols)
        unique = flip(prob["uniq_p"])
        where = Where.random(table, no_sub=True, param_prob=prob) if flip(prob["where_p"]) else None
        return Index(name=name, table=table.name, columns=cols, unique=unique, where=where)
    
    def mutate(self, table: Optional["Table"] = None) -> "Index":
        idx = copy.deepcopy(self)
        mutation_type = random.choice(["rename", "toggle_unique", "change_col", "change_where", "toggle_where"])

        if mutation_type == "rename":
            idx.name += f"_{random.choice(['v2', 'alt', 'x'])}"

        elif mutation_type == "toggle_unique":
            idx.unique = not idx.unique

        elif mutation_type == "change_col" and table is not None and col_names:
            col_names = table.get_col_names()
            if col_names:
                num_cols = random.randint(1, min(len(col_names), 3))
                idx.columns = random.sample(col_names, num_cols)

        elif mutation_type == "change_where":
            idx.where = idx.where.mutate() if idx.where else None

        elif mutation_type == "toggle_where" and table is not None:
            idx.where = Where.random(table) if not idx.where else None

        return idx
    
@dataclass
class Trigger(SQLNode):
    '''
    CREATE TRIGGER name BEFORE/AFTER INSERT/UPDATE/DELETE ON table
    BEGIN
        INSERT/UPDATE/DELETE ...
    END
    '''
    name: str
    temp: bool
    nexists: bool
    timing: str
    event: str
    table: Table
    foreach: bool
    statements: List[str] 
    cols: Optional[List[str]] = None
    when: Optional[Where] = None #when inside a trigger needs to reference columns with NEW and OLD

    def sql(self) -> str:
        base = "CREATE "
        if self.temp:
            base += "TEMP "
        base += "TRIGGER "
        if self.nexists:
            base+= "IF NOT EXISTS "
        base += f"{self.name} {self.timing} {self.event} " 
        if self.cols:
           base += f"OF {', '.join(self.cols)} " 
        base += f"ON {self.table.name} "
        if self.foreach:
            base+= "FOR EACH ROW "
        if self.when:
            base += f"WHEN {self.when.sql()}"
            
        body = " ".join(self.statements)
        return f"{base} BEGIN {body} END"

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Trigger":
        prob = {"temp_p":0.2, "nex_p":0.2, "upcol_p":0.2, "where_trigger_p": 0.0, "feac_p":0.2, "dft_trigger_p":0, "conf_p":0.9, "rexp_trigger_p":0}
        if param_prob is not None:
            prob.update(param_prob)
        
        prob.update({
            "where_p":prob["where_trigger_p"], 
            "dft_p":prob["dft_trigger_p"],
            "rexp_p":prob["rexp_trigger_p"]
        })
            
        name = random_name("trg")
        temp = flip(prob["temp_p"])
        nexists = flip(prob["nex_p"])
        timing = random.choice(["BEFORE", "AFTER"])
        event = random.choice(["INSERT", "UPDATE", "DELETE"])
        cols = None
        if event == "UPDATE" and flip(prob["upcol_p"]):
            num_cols = random.randint(1, len(table.columns))
            sample_cols = random.sample(table.columns, num_cols)
            cols = [col.name for col in sample_cols]
        when = Where.random(table, param_prob=prob) if flip(prob["where_p"]) else None
        foreach = flip(prob["feac_p"])

        statements = []
        for _ in range(random.randint(1, 3)):
            if flip(0.25):
                statements.append(Select.random(table, param_prob=prob).sql()+";")
                continue
            insert = Insert.random(table, non_unique=True, param_prob=prob) #param_prob={"dft_p":0, "conf_p":0.9})
            if not insert or flip(0.33):
                statements.append(Delete.random(table, param_prob=prob).sql()+";")
                continue
            else:
                update = Update.random(table, param_prob=prob)
                if not update or flip(0.5):
                    statements.append(insert.sql()+";") 
                else:
                    statements.append(update.sql()+";")

        return Trigger(name=name, temp=temp, nexists=nexists, timing=timing, event=event, cols=cols, table=table, foreach=foreach, when=when, statements=statements)

    def mutate(self) -> "Trigger":
        trg = copy.deepcopy(self)
        mutation_type = random.choice([
            "rename", "toggle_temp", "toggle_nexists", "toggle_foreach",
            "change_timing", "change_event"
        ])

        if mutation_type == "rename":
            trg.name += f"_{random.choice(['v2', 'alt', 'x'])}"

        elif mutation_type == "toggle_temp":
            trg.temp = not trg.temp

        elif mutation_type == "toggle_nexists":
            trg.nexists = not trg.nexists

        elif mutation_type == "toggle_foreach":
            trg.foreach = not trg.foreach

        elif mutation_type == "change_timing":
            trg.timing = "AFTER" if trg.timing == "BEFORE" else "BEFORE"

        elif mutation_type == "change_event":
            trg.event = random.choice([e for e in ["INSERT", "UPDATE", "DELETE"] if e != trg.event])

        #elif mutation_type == "mutate_when":
        #    trg.when = Where.random(trg.table)

        return trg

#----------------------------------------------------------------------------------------------------------------------------------------------------#  

@dataclass
class DropTable(SQLNode):
    """
    DROP [TABLE, VIEW,...] [IF EXISTS] name
    """
    table_name: str
    if_exists: bool
    table_type: str
    fake_table: bool = False
    
    def sql(self) -> str:
        return f"DROP {self.table_type} {'IF EXISTS' if self.if_exists else ''} {self.table_name}"
    
    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "DropTable":
        prob = {"ifex_p":0, }
        if param_prob is not None:
            prob.update(param_prob)
        
        if_exists=flip(prob["ifex_p"])
        
        if if_exists and flip(prob["fktbl_p"]):
            fake_name = random_name(prefix="faketable")
            fake_type = random.choice(["TABLE", "TRIGGER", "VIEW", "INDEX"])
            return DropTable(table_name=fake_name, if_exists=True, table_type=fake_type, fake_table=True)
        
        if isinstance(table, Trigger):
            table_type = "TRIGGER"
        elif isinstance(table, View):
            table_type = "VIEW"
        elif isinstance(table, Index):
            table_type = "INDEX"
        else: 
            table_type = "TABLE"
        return DropTable(table_name=table.name, if_exists=if_exists, table_type=table_type)
        
        
#----------------------------------------------------------------------------------------------------------------------------------------------------#  

@dataclass
class Pragma(SQLNode):
    '''
    PRAGMA ...
    basically SQLite configuration
    '''
    name: str
    value: str

    def sql(self) -> str:
        if self.value:
            return f"PRAGMA {self.name} = {self.value}"
        else:
            return f"PRAGMA {self.name}"

    @staticmethod
    def random() -> "Pragma":
        pragmas = [
            ("foreign_keys", random.choice(["ON", "OFF"])),
            ("cache_size", str(random.randint(5000, 100000))),
            ("journal_mode", random.choice(["DELETE", "TRUNCATE", "PERSIST", "WAL", "MEMORY"])),
            ("synchronous", random.choice(["0", "1", "2"])),  # OFF, NORMAL, FULL
            ("temp_store", random.choice(["DEFAULT", "FILE", "MEMORY"])),
            ("locking_mode", random.choice(["NORMAL", "EXCLUSIVE"])),
            ("mmap_size", str(random.randint(10000000, 100000000))),
            ("analysis_limit", str(random.randint(1, 20))),
            ("automatic_index", random.choice(["True", "False"])),
            ("busy_timeout", str(random.randint(1000, 10000))),
            ("collation_list", ""),
            ("database_list", ""),
            ("encoding", random.choice(["", "\'UTF-8\'", "\'UTF-16\'", "\'UTF-16le\'", "\'UTF-16be\'"])),
            ("function_list", ""),
            ("recursive_triggers", random.choice(["ON", "OFF"])),
            ("case_sensitive_like", random.choice(["ON", "OFF"])),
            ("secure_delete", random.choice(["0", "1"])),
            ("page_size", str(random.choice([1024, 2048, 4096, 8192]))),
            ("max_page_count", str(random.randint(1000, 100000))),
            ("user_version", str(random.randint(0, 4294967295))),
            ("schema_version", str(random.randint(0, 4294967295))),
            ("wal_autocheckpoint", str(random.randint(0, 1000))),
            ("journal_size_limit", str(random.randint(0, 104857600))),
            ("reverse_unordered_selects", random.choice(["ON", "OFF"])),
        ]

        name, value = random.choice(pragmas)
        return Pragma(name=name, value=value)
    
@dataclass
class TransactionControl(SQLNode):
    """
    Tansactions controls in SQL
    """
    statement: str
    transaction_active: bool
    save_name: Optional[str] = None
    release: Optional[str] = None
    
    def sql(self) -> str:
        return self.statement
    
    @staticmethod
    def random(transaction_active:bool = False, save_points: List[str]=[], param_prob:Dict[str, float] = None):
        prob = {"rollback_p":0, "save_p":0.25, "release_p":0.5}
        if param_prob is not None:
            prob.update(param_prob)
        
        if not transaction_active:
            statement = random.choice(["BEGIN","BEGIN TRANSACTION"])
            return TransactionControl(statement=statement, transaction_active=True)
        
        if flip(prob["rollback_p"]):
            if flip() and save_points:
                return TransactionControl(statement=f"ROLLBACK TO {random.choice(save_points)}", transaction_active=False)
            return TransactionControl(statement="ROLLBACK", transaction_active=False)
        
        if flip(prob["save_p"]):
            save_name = random_name(prefix='save')
            return TransactionControl(statement=f"SAVEPOINT {save_name}", transaction_active=True, save_name=save_name)
        
        if save_points and flip(prob["release_p"]):
            save_name = random.choice(save_points)
            return TransactionControl(statement=f"RELEASE SAVEPOINT {save_name}", transaction_active=True, release=save_name)
        
        return TransactionControl(statement="COMMIT", transaction_active=False)

@dataclass
class Optimization(SQLNode):
    """
    Optimization statements in SQL
    """
    statement:str
    
    def sql(self) -> str:
        return self.statement
    
    @staticmethod
    def random(table: "Table"):
        statement = random.choice([
            "ANALYZE", 
            "VACUUM", 
            "REINDEX", 
            f"ANALYZE {table.name}",
            f"REINDEX {table.name}"
            ])
        return Optimization(statement=statement)
    
#----------------------------------------------------------------------------------------------------------------------------------------------------#

def randomQueryGen(query: List[str] = [], param_prob: Dict[str, float] = None, debug: bool = False, 
                   cycle: int = 3, context: List[Table] = []) -> tuple[List[str], List[Table]]:
    """
    Randomly generates the entire query, keeping track of the tables to pass as arguments.
    
    Args:
        prob (Dict[str, float]): Dictionary with probabilities of generating each query type
        debug (bool, optional): helps debugging. Defaults to False.
        cycle (int, optional): number of iterations. Defaults to 3.
        context (Table): table context to add to query. Defaults to None

    Returns:
        str: the string of the entire query generated
        
    """
    prob = {   
        "table":   0.2,
        "insert":  0.2,
        "update":  0.2,
        "replace": 0.2,
        "delete":  0.2,
        "alt_ren": 0.2, 
        "alt_add": 0.2,
        "alt_col": 0.2,
        "select1": 0.2,
        "select2": 0.2,
        "with":    0.2,
        "view":    0.2,
        "index":   0.2,
        "trigger": 0.2,
        "pragma":  0.2,
        "control": 0.01,
        "optimize":0.01,
        "drop_tbl":0.05,
    }
    
    if param_prob is not None:
        prob.update(param_prob)
    
    query = query
    tables = context
    if not context:
        table = Table.random()
        tables.append(table)
        query.append(table.sql() + ";")
        for i in range(1):
            insert = Insert.random(table, param_prob=prob)
            query.append(insert.sql() + ";")
            
    views = []
    triggers = []
    indexes = []
    transaction_active = False
    save_points = []
    for i in range(cycle):
        # try:
            if flip(prob["pragma"]) or debug:
                pragma = Pragma.random()
                query.append(pragma.sql() + ";")
                
            if flip(prob["table"]) or tables == [] or debug:
                new_table = Table.random()
                tables.append(new_table)
                query.append(new_table.sql() + ";")
                for i in range(1):
                    insert = Insert.random(new_table, param_prob=prob)
                    query.append(insert.sql() + ";")
                    
            table = random.choice(tables)
                
            if flip(prob["insert"]) or debug:
                insert = Insert.random(table, param_prob=prob)
                query.append(insert.sql() + ";")
            if flip(prob["replace"]) or debug:
                replace = Replace.random(table, param_prob=prob)
                query.append(replace.sql() + ";")
            if flip(prob["update"]) or debug:
                update = Update.random(table, param_prob=prob)
                if update:
                    query.append(update.sql() + ";")
            if flip(prob["delete"]) or debug:
                delete = Delete.random(table, param_prob=prob)
                query.append(delete.sql() + ";")
                
            if flip(prob["alt_ren"]) or debug and not table.viewed:
                new_table = AlterTable.random_tbl_rename(table)
                if new_table:
                    tables.remove(table) # renamed name of table, table does not exist anymore
                    tables.append(new_table)
                    query.append(new_table.sql() + ";")
                    table = random.choice(tables)
            if flip(prob["alt_add"]) or debug and not table.viewed:
                new_table = AlterTable.random_add(table)
                if new_table:
                    new_table.confirm_add()
                    query.append(new_table.sql() + ";")
            if flip(prob["alt_col"]) or debug and not table.viewed:
                new_table = AlterTable.random_col_rename(table)
                if new_table:
                    new_table.confirm_rename()
                    query.append(new_table.sql() + ";")
            
            if flip(prob["index"]) or debug:
                index = Index.random(table, param_prob=prob)
                if index:
                    indexes.append(index)
                    query.append(index.sql() + ";")
            if flip(prob["trigger"]) or debug:
                trigger = Trigger.random(table, param_prob=prob)
                triggers.append(trigger)
                query.append(trigger.sql() + ";")
            
            table = random.choice(tables + views)
            if flip(prob["view"]) or debug:
                table.viewed = True #flags table so that we dont modify it
                view = View.random(table, param_prob=prob)
                views.append(view)
                query.append(view.sql() + ";")
                
            if flip(prob["with"]) or debug:
                with_ = With.random(table, param_prob=prob)
                query.append(with_.sql() + ";")
            
            if flip(prob["select1"]) or debug:
                select = Select.random(table, param_prob=prob)
                query.append(select.sql() + ";")
            if (flip(prob["select2"]) and len(tables) > 1) or debug:
                select2 = Select.random(table, other_tables=tables, param_prob=prob)
                query.append(select2.sql() + ";")
                
            if flip(prob["control"]) or debug:
                transaction = TransactionControl.random(transaction_active=transaction_active, save_points=save_points, param_prob=prob)
                transaction_active = transaction.transaction_active
                if transaction.save_name:
                    save_points.append(transaction.save_name)
                if transaction.release:
                    save_points.remove(transaction.release)
                query.append(transaction.sql() + ";")
                
            if (flip(prob["optimize"]) or debug) and not transaction_active:
                optimization = Optimization.random(table)
                query.append(optimization.sql() + ";")
                
            if flip(prob["drop_tbl"]):
                table = random.choice(tables + views + triggers + indexes)
                if getattr(table, "viewed", False):
                    continue
                droptable = DropTable.random(table, param_prob=prob)
                query.append(droptable.sql() + ";")
                if not droptable.fake_table:
                    if droptable.table_type == "VIEW":
                        views.remove(table)
                    elif droptable.table_type == "TRIGGER":
                        triggers.remove(table)
                    elif droptable.table_type == "INDEX":
                        indexes.remove(table)
                    else:
                        tables.remove(table)
                
        
        # except Exception as e:
        #     print(e)
        #     i-=1

    return query, tables
        
if __name__ == "__main__":
    table = Table.random()
    select = Select.random(table)
    print(select.sql())
    for i in range(5):
        print(select.mutate().sql())

    #print(randomQueryGen(prob, debug=False, cycle=1))

