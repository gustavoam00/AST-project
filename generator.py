import random
import string
from dataclasses import dataclass
from typing import List, Optional, Union, Dict

random.seed(42)

SQL_TYPES = ["INTEGER", "TEXT", "REAL"]
SQL_CONSTRAINTS = ["PRIMARY KEY", "UNIQUE", "NOT NULL", "CHECK", "DEFAULT"]
VALUES = { # put interesting values to test here
    "INTEGER": [0, 1, -1, 
                2**31-1, -2**31, 
                2**63-1, -2**63,
                9999999999999999999,
                42, 1337,
                0x7FFFFFFF, 0x80000000,
                ],
    "TEXT": ["", " ", "a", "abc", " "*1000, 
             "' OR 1=1; --", "\x00\x01\x02", 
             "DROP TABLE test;", #"A"*10000,
             "\"quoted\"", "'quoted'", "NULL",
             ],
    "REAL": [0.0, -0.0, 1.0, -1.0,
             3.14159, 2.71828,
             float('inf'), float('-inf'), float('nan'),
             1e-10, 1e10, 1e308, -1e308,
             ]
}
CALLABLE_VALUES = {
    "INTEGER": lambda: random.randint(-1000, 1000),
    "TEXT": lambda: random_name(prefix = "v", length=5),
    "REAL": lambda: random.uniform(-1e5, 1e5)
}
OPS = {
    "INTEGER": ["=", "!=", ">", "<", ">=", "<="],
    "TEXT": ["=", "!="],
    "REAL": ["=", "!=", ">", "<", ">=", "<="],
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
    suffix = ''.join(random.choices(string.ascii_lowercase, k=length))
    return f"{prefix}_{suffix}"

def random_type() -> str:
    """
    Returns a random data type

    Returns:
        str: the dtype
    """
    return random.choice(SQL_TYPES)

def random_value(dtype:str) -> str:
    """
    Returns a ramdom value given the type, could be either interesting values or completely random.

    Args:
        dtype (str): The data type of the value

    Returns:
        str: the generated value
    """
    if dtype in SQL_TYPES:
        if flip(0.2):
            return str(CALLABLE_VALUES[dtype]())
        else:
            return str(random.choice(VALUES[dtype]))
    else:
        return "NULL"
    
def flip(weight:float = 0.5) -> bool:  
    """
    Flipping weighted coin

    Args:
        weight (float, optional): Likelihood. Defaults to 0.5.

    Returns:
        bool: success
    """
    return random.random() < weight

class SQLNode:
    def sql(self) -> str:
        raise NotImplementedError

@dataclass
class Predicate(SQLNode):
    def sql(self) -> str:
        raise NotImplementedError
    
    @staticmethod
    def random(table: "Table") -> "Predicate":
        col = random.choice(table.columns)
        predicate_classes = [NullCheck, Comparison, InList, Exists]
        table_needed = [Exists,]
        
        if col.dtype == "INTEGER":
            predicate_classes.append(Between)
        if col.dtype == "REAL":
            predicate_classes.append(Between)
        if col.dtype == "TEXT":
            predicate_classes.append(Like)
        
        cls = random.choice(predicate_classes)
        if cls in table_needed:
            return cls.random(table)
        else:
            return cls.random(col)
    
@dataclass
class Comparison(Predicate):
    '''
    column OP value
    '''
    column: str
    operator: str
    value: str
    
    def sql(self) -> str:
        return f"{self.column} {self.operator} {self.value}"

    @staticmethod
    def random(col: "Column") -> "Comparison":
        dtype = col.dtype
        op = random.choice(OPS[dtype])
        val = random_value(dtype)
        return Comparison(col.name, op, val)

@dataclass
class Between(Predicate):
    '''
    column BETWEEN low AND high
    '''
    column: str
    lower: str
    upper: str

    def sql(self) -> str:
        return f"{self.column} BETWEEN {self.lower} AND {self.upper}"

    @staticmethod
    def random(col: "Column") -> "Between":
        v1 = random_value(col.dtype)
        v2 = random_value(col.dtype)
        low, high = sorted([v1, v2], key=lambda x: float(x))
        return Between(col.name, low, high)

@dataclass
class Like(Predicate):
    '''
    column LIKE val
    '''
    column: str
    val: str

    def sql(self) -> str:
        return f"{self.column} LIKE {self.val}"

    @staticmethod
    def random(col: "Column") -> "Like":
        val = Like.generate_like_pattern(random_value("TEXT"))
        return Like(col.name, val)
    
    @staticmethod
    def generate_like_pattern(base: str) -> str:
        '''
        Helper function to create patterns for LIKE, could be more complex if we wanted
        '''
        if not base:
            return random.choice(['%', '_', ''])

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

        return random.choice(patterns)
    
@dataclass
class InList(Predicate):
    '''
    column in (v1, v2, v3, ...)
    '''
    column: str
    values: List[str]

    def sql(self) -> str:
        value_list = ', '.join(self.values)
        return f"{self.column} IN ({value_list})"

    @staticmethod
    def random(col: "Column") -> "InList":
        count = random.randint(2, 5)
        values = [random_value(col.dtype) for _ in range(count)]
        return InList(col.name, values)
    
@dataclass
class Exists(Predicate):
    '''
    EXISTS (SELECT 1 FROM ... WHERE ...)
    apparently select 1 acts the same as any other column so we probably dont need to add the functionality
    '''
    select: "Select"

    def sql(self) -> str:
        return f"EXISTS ({self.select})"

    @staticmethod
    def random(table: "Table") -> "Exists":
        select = Select.random(table, rand_where=1, sample=1)
        return Exists(select)

@dataclass
class NullCheck(Predicate):
    '''
    column IS/IS NOT NULL
    '''
    column: str
    check: bool

    def sql(self) -> str:
        if self.check:
            return f"{self.column} IS NULL"
        else:
            return f"{self.column} IS NOT NULL"

    @staticmethod
    def random(col: "Column") -> "NullCheck":
        check = flip()
        return NullCheck(col.name, check)
    
@dataclass
class Where(SQLNode):
    '''
    WHERE BooleanExpr|Comparison|InSubquery
    '''
    def sql(self) -> str:
        raise NotImplementedError 

    @staticmethod
    def random(table: "Table", max_depth: int = 3, p_depth: float = 0.3, 
               other_tables: List["Table"] = None, p_sub: float = 0.3) -> "Where":
        other_tables = other_tables or []

        if max_depth <= 0 or flip(p_depth):
            if other_tables and random.random() < 1:#and always true?
                return InSubquery.random(table, other_tables)
            return Predicate.random(table) #changed from comparison to predicate
        else:
            left = Where.random(table, max_depth - 1, p_depth)
            right = Where.random(table, max_depth - 1, p_depth)
            op = random.choice(["AND", "OR"])
            return BooleanExpr(left, right, op)

@dataclass
class InSubquery(Where):
    '''
    InSubquery: WHERE col IN (SELECT other_col FROM other_table WHERE ...)
    '''
    column: "Column" 
    subquery: "Select"

    def sql(self) -> str: 
        return f"{self.column.name} IN ({self.subquery.sql()})" #is this missing the WHERE at the beginning of the string?

    @staticmethod
    def random(table: "Table", other_tables: List["Table"]) -> "InSubquery":
        column = random.choice(table.columns)
        other_table = random.choice(other_tables)

        matching_columns = [col for col in other_table.columns if col.dtype == column.dtype]
        if not matching_columns:
            sub_col = random.choice(other_table.columns)
        else:
            sub_col = random.choice(matching_columns)
            
        where_clause = Where.random(other_table, max_depth=2)
        subquery = Select(
            columns=[sub_col],
            from_clause=other_table,
            where=where_clause
        )

        return InSubquery(column=column, subquery=subquery)

@dataclass
class BooleanExpr(Where):
    '''
    Example: a < b
    '''
    left: Where
    right: Where
    operator: str 

    def sql(self) -> str:
        return f"({self.left.sql()} {self.operator} {self.right.sql()})"

@dataclass
class Column:
    name: str
    dtype: str
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    check: Optional[Predicate] = None #changed from comparison to predicate
    default: Optional[str] = None

    def sql(self) -> str:
        col = [self.name, self.dtype]
        if self.primary_key:
            col.append("PRIMARY KEY")
        if not self.nullable:
            col.append("NOT NULL")
        if self.unique:
            col.append("UNIQUE")
        if self.check:
            col.append(f"CHECK({self.check.sql()})")
        if self.default:
            col.append(f"DEFAULT {self.default}")
        return " ".join(col)

    @staticmethod
    def random(name: Optional[str] = None, allow_pk: bool = True) -> "Column":
        name = name or random_name("col")
        dtype = random_type()
        primary_key = allow_pk and flip(0.3)
        nullable = not primary_key and random.choice([True, False])
        unique = not primary_key and flip(0.1)

        check = None
        if flip(0.3):
            temp_table = Table(name="fake", columns=[])
            fake_col = Column(name=name, dtype=dtype)
            temp_table.columns.append(fake_col)
            check = Predicate.random(temp_table) ##changed from comparison to predicate

        default = None
        if (dtype == "INTEGER" or dtype == "REAL") and flip(0.2):
            default = str(random.randint(0, 100))
        elif dtype == "TEXT" and flip(0.2):
            default = f"'{random_name()[:5]}'"

        return Column(name, dtype, primary_key, nullable, unique, check, default)

@dataclass
class Table(SQLNode):
    '''
    CREATE TABLE ...
    '''
    name: str
    columns: List[Column]

    def sql(self) -> str:
        column_defs = ", ".join([col.sql() for col in self.columns])
        return f"CREATE TABLE {self.name} ({column_defs})"

    def get_col_names(self) -> List[str]:
        return [col.name for col in self.columns]
    
    @staticmethod
    def random(name: Optional[str] = None, min_cols: int = 2, max_cols: int = 6) -> "Table":
        name = name or random_name("tbl")
        dtype_counts = {"INTEGER": 0, "TEXT": 0, "REAL": 0}
        num_cols = random.randint(min_cols, max_cols)

        columns = []
        for i in range(num_cols):
            col = Column.random(allow_pk=(i==0)) # primary key only for first column
            col_type = col.dtype
            new_name = col_type[0].lower() + str(dtype_counts[col_type])
            col.name = new_name
            if col.check:
                col.check.column = new_name
            dtype_counts[col_type] += 1
            columns.append(col)

        return Table(name, columns)
    
@dataclass
class AlterTable(Table):
    name: str
    old_name: str = ""
    new_col: Optional[Column] = None
    mod_col: Optional[Column] = None

    def sql(self) -> str:
        if self.mod_col:
            return f"ALTER TABLE {self.name} RENAME {self.mod_col.name} TO {self.new_col.name};"
        elif self.new_col:
            return f"ALTER TABLE {self.name} ADD COLUMN {self.new_col.name};"
        else:
            return f"ALTER TABLE {self.old_name} RENAME TO {self.name}"
        
    @staticmethod
    def random_add(table: "Table") -> "AlterTable":
        new_col = Column.random()
        modified_col = table.columns
        modified_col.append(new_col)
        return AlterTable(name=table.name, new_col=new_col, columns=modified_col)
    
    @staticmethod
    def random_col_rename(table: "Table") -> "AlterTable":
        modified_cols = table.columns
        mod_col = random.choice(modified_cols)
        old_col = Column(name=mod_col.name, dtype=mod_col.dtype)
        mod_col.name = random_name("col")
        return AlterTable(name=table.name, new_col=mod_col, mod_col=old_col, columns=modified_cols)
    
    @staticmethod
    def random_tbl_rename(table: "Table") -> "AlterTable":
        return AlterTable(name=random_name("tbl"), old_name= table.name, columns=table.columns)

@dataclass
class Insert(SQLNode):
    """
    Returning only for SQLite 3.35+ so no?
    """
    table: str
    columns: List[Column]
    values: List[List[str]]
    conflict_action: Optional[str] = None
    default: bool
    full:bool

    def sql(self) -> str:
        query = "INSERT "
        if self.conflict_action:
            query+= f"OR {self.conflict_action} "
            
        if self.default:
            return query+f"INTO {self.table} DEFAULT VALUES"
        
        cols = ", ".join([c.name for c in self.columns])
        vals_list = []
        for row in self.values:
            vals = ", ".join(row)
            vals_list.append(f"({vals})")
        all_vals = ", ".join(vals_list)
            
        if self.full:
            query+= f"INTO {self.table} VALUES {all_vals}"
        else:
            query += f"INTO {self.table} ({cols}) VALUES {all_vals}"
        return query

    @staticmethod
    def random(table: "Table") -> "Insert":
        default = flip(0.2)
        conflict_action = random.choice(["ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"]) if flip(0.2) else None
        full = flip(0.2)
        
        if default:
            return Insert(table=table.name, columns=[], values=[], conflict_action=conflict_action, default=True, full=False)
        
        num_cols = random.randint(1,len(table.columns)) if not full else len(table.columns)
        cols = []
        num_rows = random.randint(1,5)
        vals = [[] for _ in range(num_rows)]

        for col in random.sample(table.columns, num_cols):
            cols.append(col)
            for i in range(num_rows):
                vals[i].append(random_value(col.dtype))

        return Insert(table=table.name, columns=cols, values=vals, conflict_action=conflict_action, default=default, full=full)
    
@dataclass
class Update(SQLNode):
    table: str
    columns: List[Column]
    values: List[str]
    where: Optional[Where] = None

    def sql(self) -> str:
        query = f"UPDATE {self.table} SET "
        assignments = []
        for column, value in zip(self.columns, self.values):
            assignments.append(f"{column.name} = {value}")
        query += ", ".join(assignments)
        if self.where:
            query += f" WHERE {self.where.sql()}"
        return query

    @staticmethod
    def random(table: "Table") -> "Update":
        cols = []
        vals = []

        for col in table.columns:
            cols.append(col)
            vals.append(random_value(col.dtype))

        where = Where.random(table) if flip(0.4) else None
        return Update(table=table.name, columns=cols, values=vals, where=where)
    
@dataclass   
class Delete(SQLNode):
    table: str
    where: Optional[Where] = None

    def sql(self) -> str:
        query = f"DELETE FROM {self.table}"
        if self.where:
            query += f" WHERE {self.where.sql()}"
        return query

    @staticmethod
    def random(table: "Table") -> "Delete":
        where = Where.random(table) if flip(0.95) else None
        return Delete(table=table.name, where=where)
    
@dataclass
class Replace(SQLNode):
    table: str
    columns: List[Column]
    values: List[List[str]]
    default: bool
    full:bool

    def sql(self) -> str:
        query = "REPLACE "
        
        if self.default:
            return query+f"INTO {self.table} DEFAULT VALUES"
        
        cols = ", ".join([c.name for c in self.columns])
        vals_list = []
        for row in self.values:
            vals = ", ".join(row)
            vals_list.append(f"({vals})")
        all_vals = ", ".join(vals_list)
            
        if self.full:
            query+= f"INTO {self.table} VALUES {all_vals}"
        else:
            query += f"INTO {self.table} ({cols}) VALUES {all_vals}"
        return query

    @staticmethod
    def random(table: "Table") -> "Replace":
        default = flip(0.2)
        full = flip(0.2)
        
        if default:
            return Replace(table=table.name, columns=[], values=[], default=True, full=False)
        
        num_cols = random.randint(1,len(table.columns)) if not full else len(table.columns)
        cols = []
        num_rows = random.randint(1,5)
        vals = [[] for _ in range(num_rows)]

        for col in random.sample(table.columns, num_cols):
            cols.append(col)
            for i in range(num_rows):
                vals[i].append(random_value(col.dtype))

        return Replace(table=table.name, columns=cols, values=vals, default=default, full=full)
    
@dataclass
class Join(SQLNode):
    '''
    table1 INNER JOIN table2 
    '''
    left_table: Table
    right_table: Table
    left_column: Column
    right_column: Column
    join_type: str

    def sql(self) -> str:
        return (
            f"{self.left_table.name} {self.join_type} JOIN {self.right_table.name} " +
            f"ON {self.left_table.name}.{self.left_column.name} = {self.right_table.name}.{self.right_column.name}"
        )

    @staticmethod
    def random(left: "Table", right: "Table", join_type: str = None) -> "Join":
        left_cols = [c for c in left.columns if c.dtype in {"INTEGER", "TEXT"}]
        right_cols = [c for c in right.columns if c.dtype in {"INTEGER", "TEXT"}]

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
            join_type=join_type
        )

@dataclass
class Select(SQLNode):
    columns: List[Column]
    from_clause: Union[Table, Join]
    where: Optional[Where] = None
    group_by: Optional[List[Column]] = None
    order_by: Optional[List[Column]] = None
    #having: #this one is confucing, its like a group level condition generally after a group by
    limit: Optional[int] = None
    offset: Optional[int] = None

    def sql(self) -> str:
        col_names = [c.name for c in self.columns]
        base = f"SELECT {', '.join(col_names)} FROM {self.from_clause.sql() if isinstance(self.from_clause, Join) else self.from_clause.name}"
        if self.where:
            base += f" WHERE {self.where.sql()}"
        if self.group_by:
            group_names = [c.name for c in self.group_by]
            base += f" GROUP BY {', '.join(group_names)}"
        if self.order_by:
            order_names = [c.name for c in self.order_by]
            base += f" ORDER BY {', '.join(order_names)}"
        if self.limit:
            base += f" LIMIT {self.limit}"
            if self.offset:
                base+= f" OFFSET {self.offset}"
                
        return base

    @staticmethod
    def random(
        table: Table,
        sample: int = None,
        rand_where: float = 0.9,
        rand_group: float = 0.3,
        rand_order: float = 0.3,
        rand_join: float = 0.3,
        rand_limit: float = 0.2,
        other_tables: list[Table] = None,
    ) -> "Select":
        cols = table.columns
        if not sample:
            selected_cols = random.sample(cols, random.randint(1, len(cols)))
        else:
            selected_cols = random.sample(cols, sample)

        if other_tables and flip(rand_join):
            left = table
            right = random.choice(other_tables)
            from_clause = Join.random(left, right)
        else:
            from_clause = table

        where = Where.random(table, other_tables=other_tables) if flip(rand_where) else None
        group_by = random.sample(selected_cols, k=1) if flip(rand_group) else None
        order_by = random.sample(selected_cols, k=1) if flip(rand_order) else None
        limit = random.randint(1,20) if flip(rand_limit) else None
        offset = random.randint(1,20) if flip() and limit else None

        return Select(
            columns=selected_cols,
            from_clause=from_clause,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )
    
    @staticmethod
    def random_with_join(left: Table, right: Table, rand_where: float = 0.5) -> "Select":
        join_clause = Join.random(left, right)

        combined_cols = left.columns + right.columns
        selected_cols = random.sample(combined_cols, k=random.randint(1, len(combined_cols)))
        where = Where.random(left) if flip(rand_where) else None

        return Select(
            columns=selected_cols,
            from_clause=join_clause,
            where=where
        )

@dataclass
class View(Table):
    name: str
    select: Select
    columns: List[Column]

    def sql(self) -> str:
        return f"CREATE VIEW {self.name} AS ({self.select.sql()})"
    
    @staticmethod
    def random(table: Table) -> "View":
        view_name = random_name("view")
        select = Select.random(table)
        return View(name=view_name, columns=select.columns, select=select)
        
@dataclass
class With(SQLNode):
    name: str
    query: SQLNode
    recursive: bool = False
    main_query: Optional[SQLNode] = None

    def sql(self) -> str:
        rec = "RECURSIVE " if self.recursive else ""
        full_query = f"WITH {rec}{self.name} AS ({self.query.sql()})"
        if self.main_query:
            return f"{full_query} {self.main_query.sql()}"
        return full_query
    
    @staticmethod
    def random(table: Table) -> "With":
        with_name = random_name("with")
        with_table = Table(with_name, table.columns)
        inner_select = Select.random(table)
        main_select = Select(
            columns=inner_select.columns,
            from_clause=with_table
        )
        return With(
            name=with_name,
            query=inner_select,
            main_query=main_select
        )

@dataclass
class Index(SQLNode):
    name: str
    table: str
    columns: List[str]
    unique: bool = False
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
    def random(table: "Table") -> "Index":
        name = random_name("idx")
        col_names = table.get_col_names()
        num_cols = random.randint(1, min(len(col_names), 3))
        cols = random.sample(col_names, num_cols)
        unique = flip(0.3)
        where = Where.random(table) if flip(0.4) else None
        return Index(name=name, table=table.name, columns=cols, unique=unique, where=where)
    
@dataclass
class Trigger(SQLNode):
    name: str
    timing: str
    event: str 
    table: Table
    when: Optional[Where] = None
    statements: List[SQLNode] = None 

    def sql(self) -> str:
        base = f"CREATE TRIGGER {self.name} {self.timing} {self.event} ON {self.table.name}"
        if self.when:
            base += f" WHEN {self.when.sql()}"
        body = ""
        for stmt in self.statements:
            if isinstance(stmt, Insert):
                body += stmt.sql()
            else:
                body += ""
        return f"{base} BEGIN\n{body}\nEND;"

    @staticmethod
    def random(table: "Table") -> "Trigger":
        name = random_name("trg")
        timing = random.choice(["BEFORE", "AFTER"])
        event = random.choice(["INSERT", "UPDATE", "DELETE"])
        when = Where.random(table) if flip(0.4) else None

        body = []
        for _ in range(random.randint(1, 2)):
            body.append(Insert.random(table)) 

        return Trigger(name, timing, event, table, when, body)

@dataclass
class Expression(SQLNode):
    """
    Expression is anything that computes a value, including:
    Column - col
    Literal - 'USA', 42, 3.14, NULL	Constant values
    Math/Logic - col * 1.1, age + 5, x > 10
    String Ops - LOWER(name), SUBSTR(city, 1, 3)	
    CASE - CASE WHEN x > 0 THEN 'Yes' ...
    Aggregate Funcs	- SUM(sales), COUNT(*) - Only in SELECT, HAVING, etc.
    Cast/Conversion - CAST(age AS TEXT)	
    Nested SELECT - (SELECT MAX(age) FROM users) - Scalar subquery — returns 1 value
    
    Put it inside select, where and update
    """
    def sql(self) -> str:
        return NotImplementedError
    
    @staticmethod
    def random():
        return
    
@dataclass
class Case(SQLNode):
    """
    A Expression, cant be used alone
    Put it inside select, where, order by, and update
    """
    conditions:List[str]
    values:List[str]
    col:str
    else_:str
    
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
    def random(table: "Table", column:Optional["Column"]=None, case_dtype:Optional[str]=None) -> "Case":
        if column:
            col = column.name
        else:
            column = random.choice(table.columns)
            col = "" if flip() else f"{column.name}"
        col_dtype = column.dtype
        
        if not case_dtype:
            case_dtype = random_type()
            
        num_cases = random.randint(1,5)
        conditions = []
        values = []
        for _ in range(num_cases):
            if col:
                conditions.append(random_value(col_dtype))
            else:
                conditions.append(Comparison.random(table))
                
            values.append(random_value(case_dtype))
        
        else_ = "" if flip() else random_value(case_dtype)
        return Case(conditions, values, col, else_)
    

    
       



def randomQueryGen(prob: Dict[str, float], debug: bool = False, cycle: int = 10) -> str:
    """
    Randomly generates the entire query, keeping track of the tables to pass as arguments.
    
    Args:
        prob (Dict[str, float]): Dictionary with probabilities of generating each query type
        debug (bool, optional): helps debugging. Defaults to False.
        cycle (int, optional): number of iterations. Defaults to 10.

    Returns:
        str: the string of the entire query generated
        
    """
    tables = [Table.random()]
    query = tables[0].sql() + "\n"
    for i in range(cycle):
        table = random.choice(tables)
        if random.random() < prob["alt_ren"] or debug:
            new_table = AlterTable.random_tbl_rename(table)
            tables.remove(table) # renamed name of table, table does not exist anymore
            tables.append(new_table)
            table = new_table
            query += table.sql() + "\n"
        if random.random() < prob["alt_add"] or debug:
            table = AlterTable.random_add(table)
            query += table.sql() + "\n"
        if random.random() < prob["alt_col"] or debug:
            table = AlterTable.random_col_rename(table)
            query += table.sql() + "\n"
        if random.random() < prob["sel1"] or debug:
            query += Select.random(table).sql() + "\n"
        if random.random() < prob["with"] or debug:
            query += With.random(table).sql() + "\n"
        if random.random() < prob["view"] or debug:
            new_table = View.random(table)
            tables.append(new_table)
            query += new_table.sql() + "\n"
        if random.random() < prob["idx"] or debug:
            query += Index.random(table).sql() + "\n"
        if random.random() < prob["trg"] or debug:
            query += Trigger.random(table).sql() + "\n"
        if (random.random() < prob["sel2"] and len(tables) > 1) or debug:
            #if table in tables:
            #    tables.remove(table)
            query += Select.random(table, other_tables=tables).sql() + "\n"
            #tables.append(table)

    return query
        
if __name__ == "__main__":
    prob = {   
        "alt_ren": 0.1, 
        "alt_add": 0.1,
        "alt_col": 0.1,
        "sel1": 0.5,
        "sel2": 0.5,
        "with": 0.2,
        "view": 0.2,
        "idx": 0.1,
        "trg": 0.1 
    }
    print(randomQueryGen(prob, debug=False, cycle=10))