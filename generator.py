import random
import string
from dataclasses import dataclass
from typing import List, Optional, Union

#random.seed(42)

SQL_TYPES = ["INTEGER", "TEXT", "REAL"]
SQL_CONSTRAINTS = ["PRIMARY KEY", "UNIQUE", "NOT NULL", "CHECK", "DEFAULT"]
VALUES = {
    "INTEGER": [0, 1, -1, 
                2**31-1, -2**31, 
                2**63-1, -2**63,
                9999999999999999999,
                42, 1337,
                0x7FFFFFFF, 0x80000000],
    "TEXT": ["", " ", "a", "abc", " "*1000, 
             "' OR 1=1; --", "\x00\x01\x02", 
             "A"*10000, "DROP TABLE test;", 
             "\"quoted\"", "'quoted'", "NULL"],
    "REAL": [0.0, -0.0, 1.0, -1.0,
             3.14159, 2.71828,
             float('inf'), float('-inf'), float('nan'),
             1e-10, 1e10, 1e308, -1e308]
}
OPS = {
    "INTEGER": ["=", "!=", ">", "<", ">=", "<="],
    "TEXT": ["=", "!=", "LIKE"],
    "REAL": ["=", "!=", ">", "<", ">=", "<="]
}

def random_name(prefix: str = "x", length: int = 5) -> str:
    suffix = ''.join(random.choices(string.ascii_lowercase, k=length))
    return f"{prefix}_{suffix}"

def random_type() -> str:
    return random.choice(SQL_TYPES)

'''

'''
class SQLNode:
    def sql(self) -> str:
        raise NotImplementedError
        
@dataclass
class Comparison(SQLNode):
    column: str
    operator: str
    value: str

    def sql(self) -> str:
        return f"{self.column} {self.operator} {self.value}"

    @staticmethod
    def random(table: "Table") -> "Comparison":
        col = random.choice(table.columns)
        op = random.choice(OPS[col.dtype])

        if col.dtype == "INTEGER" or col.dtype == "REAL":
            val = str(random.choice(VALUES[col.dtype]))
        elif col.dtype == "TEXT":
            val = f"'{random.choice(VALUES[col.dtype])}'"
        else:
            val = "NULL"

        return Comparison(col.name, op, val)

@dataclass
class Where(SQLNode):
    def sql(self) -> str:
        raise NotImplementedError

    @staticmethod
    def random(table: "Table", max_depth: int = 3, p_depth: float = 0.3) -> "Where":
        if max_depth == 0 or random.random() < p_depth:
            return Comparison.random(table)
        else: #if random.random() < p_depth:
            left = Where.random(table, max_depth - 1, p_depth)
            right = Where.random(table, max_depth - 1, p_depth)
            op = random.choice(["AND", "OR"])
            return BooleanExpr(left, right, op)
        '''else:
            right = Select.random(table, sample = 1)
            right.columns[0]
            left = random.choice(VALUES)
        '''
@dataclass
class BooleanExpr(Where):
    left: "BooleanExpr"
    right: "BooleanExpr"
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
    check: Optional[Comparison] = None
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
        primary_key = allow_pk and random.random() < 0.3
        nullable = not primary_key and random.choice([True, False])
        unique = not primary_key and random.random() < 0.1

        check = None
        if random.random() < 0.3:
            temp_table = Table(name="fake", columns=[])
            fake_col = Column(name=name, dtype=dtype)
            temp_table.columns.append(fake_col)
            check = Comparison.random(temp_table)

        default = None
        if (dtype == "INTEGER" or dtype == "REAL") and random.random() < 0.2:
            default = str(random.randint(0, 100))
        elif dtype == "TEXT" and random.random() < 0.2:
            default = f"'{random_name()[:5]}'"

        return Column(name, dtype, primary_key, nullable, unique, check, default)

@dataclass
class Table(SQLNode):
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
        old_col = mod_col
        mod_col.name = random_name("col")
        return AlterTable(name=table.name, new_col=mod_col, mod_col=old_col, columns=modified_cols)
    
    @staticmethod
    def random_tbl_rename(table: "Table") -> "AlterTable":
        return AlterTable(name=random_name("tbl"), old_name= table.name, columns=table.columns)
    
@dataclass
class Insert(SQLNode):
    table: str
    columns: List[Column]
    values: List[str]

    def sql(self) -> str:
        cols = ", ".join([c.name for c in self.columns])
        vals = ", ".join(self.values)
        return f"INSERT INTO {self.table} ({cols}) VALUES ({vals});"

    @staticmethod
    def random(table: "Table") -> "Insert":
        cols = []
        vals = []

        for col in table.columns:
            cols.append(col)
            if col.dtype == "INTEGER":
                val = str(random.randint(0, 100))
            elif col.dtype == "TEXT":
                val = f"'{random_name()[:5]}'"
            elif col.dtype == "REAL":
                val = f"{random.uniform(0, 100):.2f}"
            else:
                val = "NULL"

            vals.append(val)

        return Insert(table=table.name, columns=cols, values=vals)
    
@dataclass
class Join(SQLNode):
    left_table: str
    right_table: str
    left_column: str
    right_column: str
    join_type: str = "INNER"  # or LEFT, RIGHT, FULL (SQLite supports INNER, LEFT)

    def sql(self) -> str:
        return (
            f"{self.left_table} {self.join_type} JOIN {self.right_table} " +
            f"ON {self.left_table}.{self.left_column} = {self.right_table}.{self.right_column}"
        )

    @staticmethod
    def random(left: "Table", right: "Table") -> "Join":
        left_cols = [c for c in left.columns if c.dtype in {"INTEGER", "TEXT"}]
        right_cols = [c for c in right.columns if c.dtype in {"INTEGER", "TEXT"}]

        if not left_cols or not right_cols:
            left_col = random.choice(left.columns)
            right_col = random.choice(right.columns)
        else:
            left_col = random.choice(left_cols)
            right_col = random.choice(right_cols)

        join_type = random.choice(["INNER", "LEFT"])
        return Join(
            left_table=left.name,
            right_table=right.name,
            left_column=left_col.name,
            right_column=right_col.name,
            join_type=join_type
        )

@dataclass
class Select(SQLNode):
    columns: List[Column]
    from_clause: Union[Table, Join]
    where: Optional[Where] = None
    group_by: Optional[List[Column]] = None
    order_by: Optional[List[Column]] = None

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
        return base

    @staticmethod
    def random(table: Table, rand_where: float = 0.9, rand_group: float = 0.3, rand_order: float = 0.3, sample: int = None) -> "Select":
        cols = table.columns
        if not sample:
            selected_cols = random.sample(cols, random.randint(1, len(cols)))
        else:
            selected_cols = random.sample(cols, sample)

        where = Where.random(table) if random.random() < rand_where else None
        group_by = random.sample(selected_cols, k=1) if random.random() < rand_group else None
        order_by = random.sample(selected_cols, k=1) if random.random() < rand_order else None

        return Select(
            columns=selected_cols,
            from_clause=table,
            where=where,
            group_by=group_by,
            order_by=order_by
        )
    
    @staticmethod
    def random_with_join(left: Table, right: Table, rand_where: float = 0.5) -> "Select":
        join_clause = Join.random(left, right)

        combined_cols = left.columns + right.columns
        selected_cols = random.sample(combined_cols, k=random.randint(1, len(combined_cols)))
        where = Where.random(left) if random.random() < rand_where else None

        return Select(
            columns=selected_cols,
            from_clause=join_clause,
            where=where
        )

@dataclass
class View(SQLNode):
    name: str
    query: SQLNode

    def sql(self) -> str:
        return f"CREATE VIEW {self.name} AS ({self.query.sql()})"
    
    @staticmethod
    def random(table: Table) -> "View":
        view_name = random_name("view")
        query = Select.random(table)
        return View(name=view_name, query=query)
        
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
        unique = random.random() < 0.3
        where = Where.random(table) if random.random() < 0.4 else None
        return Index(name=name, table=table.name, columns=cols, unique=unique, where=where)
    
@dataclass
class Trigger(SQLNode):
    name: str
    timing: str  # BEFORE or AFTER
    event: str  # INSERT, UPDATE, DELETE
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
                body += stmt.sql() + "\n"
            else:
                body += ""
        return f"{base} BEGIN\n{body}\nEND;"

    @staticmethod
    def random(table: "Table") -> "Trigger":
        name = random_name("trg")
        timing = random.choice(["BEFORE", "AFTER"])
        event = random.choice(["INSERT", "UPDATE", "DELETE"])
        when = Where.random(table) if random.random() < 0.4 else None

        body = []
        for _ in range(random.randint(1, 2)):
            body.append(Insert.random(table)) 

        return Trigger(name, timing, event, table, when, body)

if __name__ == "__main__":
    table = Table.random()
    print(table.sql())
    table = AlterTable.random_tbl_rename(table)
    print(table.sql())
    table = AlterTable.random_add(table)
    print(table.sql())
    table = AlterTable.random_col_rename(table)
    print(table.sql())

    select_query = Select.random(table)
    with_query = With.random(table)
    view_query = View.random(table)

    print(select_query.sql())
    print(with_query.sql())
    print(view_query.sql())

    for i in range(3):
        col = Column.random().sql()
        print(col)

    index = Index.random(table)
    print(index.sql())

    trigger = Trigger.random(table)
    print(trigger.sql())

    table2 = Table.random()
    print(table2.sql())
    join = Join.random(table, table2)
    print(join.sql())
    select_join = select_query.random_with_join(table, table2)
    print(select_join.sql())

