SEED = 42 # random seed
TEST_FOLDER = "test" # folder to save queries from fuzzer
FUZZ_FOLDER = "test/fuzz_results" # fuzzing results
ERROR = False # save errors in metric.py
SQLITE_VERSIONS = ["sqlite3-3.26.0", "sqlite3-3.39.4"]

PROB_TABLE = {
    # Comparison
    "comp_nullc": 0.05, "comp_callc": 0.9,

    # Between
    "bet_nullc" : 0, "bet_callc" : 0.9,

    # Like
    "like_nullc" : 0.05, "like_callc" : 0.9,

    # InList
    "inli_nullc" : 0.05, "inli_callc" : 0.9,

    # Exists
    "where_ex_p" : 1, "grp_ex_p" : 0, "ord_ex_p" : 0, "*_ex_p":0, "cols_ex_p":0, "one_ex_p":1, "omit_ex_p":0, "lit_ex_p":1,

    # NullCheck
    "nullc" : 0.5, 

    # Expression
    "nocol_p":0.5, "cole_p": 0.8, "lit_p":0.9, "case_p":0.05, "time_p": 0.05,

    # Literal
    "null_p":0.01, "call_p":0.9, "one_p":0.01, "std_p": 0.75, "form_p":0.3, "cast_p":0.05, "agg2_p": 0.5, "rexp_p":0.01, "alias_p":0.01,

    # ColumnExpression
    "std_p":        0.4, 
    "form_p":       0.4, 
    "cast_p":       0.2,
    "alias_p":      0.01,

    # Case
    "case_col_p" :  0.5,

    # Where
    "pred_p":       1.0, 
    "depth_p":      0.7, 
    "sub_p":        0.05, 
    #"where_p":      0.05,

    # InSubquery
    #"where_p":      0.05,

    # Column
    "pk_p":         0.0, 
    "unq_p":        0.001, 
    "dft_p":        0.2, 
    "nnl_p":        0.01, 
    "cck_p":        0.3, 
    "typeless_p":   0.1,

    # Insert
    "dft_p":        0.2, 
    "full_p":       0.2, 
    "conf_p":       0.2, 
    "null_p":       0.05, 
    "call_p":       0.9,

    # Update
    #"where_p":       0.75,

    # Delete
    #"where_p":       0.95,

    # Replace
    "dft_p":        0.2, 
    "full_p":       0.2, 
    "null_p":       0.05, 
    "call_p":       0.9,

    # Select
    "where_p":      0.9, 
    "grp_p":        0.3, 
    "ord_p":        0.3, 
    "join_p":       0.3, 
    "lmt_p":        0.2, 
    "case_p":       0.05, 
    "offst_p":      0.5, 
    "*_p":          0.2, 
    "omit_p":       0.1, 
    "one_p":        0.05, 
    "date_p":       0.1, 
    "cols_p":       0.5, 
    "agg_p":        0.1, 
    "count_p":      0.3, 
    "alias_p":      0.05,

    # With
    "rec_p":        0.5, 
    "one_with_p":   0, 
    "*_with_p":     1, 
    "select_p":     0.95,

    # View
    "tmp_p":        0.01, 
    "one_view_p":   0, 
    "*_p":          0.5, 
    "cols_view_p":  1, 
    "alias_view_p": 0, 
    "rexp_view_p":  0,
    
    # Index
    "uniq_p":           0.001, 
    #"where_p":          0.4, 
    "rexp_index_p":     0, 
    "time_index_p":     0, 
    "std_index_p":      1,

    # Trigger
    "temp_p":           0.2, 
    "nex_p":            0.2, 
    "upcol_p":          0.2, 
    "where_trigger_p":  0.0, 
    "feac_p":           0.2, 
    "dft_trigger_p":    0, 
    "conf_p":           0.9, 
    "rexp_trigger_p":   0,

    # Drop
    "ifex_p":       0,
    
    # Transactions
    "rollback_p":   0.0,
    "save_p":       0.25,
    "release_p":    0.5,

    # Dates
    "date_p":       0.1,
}
'''
    "alt_ren": 0.1, 
    "alt_add": 0.1,
    "alt_col": 0.1,
    "sel1": 0.5,
    "sel2": 0.5,
    "with": 0.2,
    "view": 0.2,
    "idx": 0.1,
    "trg": 0.1,
    "insert": 0.5,
    "update": 0.3,
    "replace": 0.2,
    "pragma": 0.1,
'''

SQL_KEYWORDS = [
    "SELECT", "FROM", "WHERE", "ORDER BY", "GROUP BY", "JOIN", 
    "LEFT JOIN", "INNER JOIN", "LIMIT", "OFFSET", "CREATE", "TABLE", 
    "VIRTUAL", "VIEW", "BETWEEN", "AS", "IN", "LIKE", "AND", "OR",
    "MATCH", "EXISTS", "EXPLAIN", "BEGIN", "END", "COMMIT", "ROLLBACK",
    "IS", "NOT", "NULL", "CASE", "WHEN", "THEN", "ELSE", "RENAME", "COLUMN",
    "VALUES", "TO", "INSERT", "INTO", "UPDATE", "DELETE", "DEFAULT", "SET"
    "REPLACE", "WITH", "USING", "INDEX", "ON", "UNIQUE", "TRIGGER",
    "BEFORE", "AFTER", "TEMP", "IF", "FOR", "EACH", "ROW", "PRAGMA", "",
    "PRIMARY", "KEY", "INTEGER", "TEXT", "REAL", "CHECK"
]

SQL_TYPES = ["INTEGER", "TEXT", "REAL"]
SQL_CONSTRAINTS = ["PRIMARY KEY", "UNIQUE", "NOT NULL", "CHECK", "DEFAULT"]
SQL_OPERATORS = ["=", "!=", "<", ">", "<=", ">=", "LIKE"]

VIRTUAL = {
    "types": ["rtree", "fts4", "dbstat"],
    "dbstat": [{"name": "TEXT"}, {"path": "TEXT"}, {"pageno": "INTEGER"}, {"pagetype": "TEXT"}, {"ncell": "INTEGER"}, {"payload": "INTEGER"}, {"unused": "INTEGER"}, {"mx_payload": "INTEGER"}, {"pgoffset": "INTEGER"}, {"pgsize": "INTEGER"}, {"schema": "TEXT"}],
    "rtree": [{"id": "INTEGER", "constraint": "PRIMARY KEY"}, {"minX": "REAL"}, {"maxX": "REAL"}, {"minY": "REAL"}, {"maxY": "REAL"}],
    "fts4": [{"title": "TEXT"}, {"body": "TEXT"}]
}

VALUES = {
    "INTEGER": [
        0, 1, -1, 
        2**31-1, -2**31, 
        2**63-1, -2**63,
        999999999999999999999999999999999999999999999999999999999999999999999999,
        42, 1337,
        0x7FFFFFFF, 0x80000000,
    ], "TEXT": [
        "''", "' '", "'a'", "'abc'", "'" + " "*50 + "'", 
        "'quoted'", "'NULL'",
    ], "REAL": [
        0.0, -0.0, 1.0, -1.0,
        3.14159, 2.71828,
        1e-10, 1e10, 1e308, -1e308,
    ]
}

TIME = {
    "TIMES": [
        "'now'",
        "'2025-01-01 12:00:00'",
        "'2020-06-15 08:45:00'",
        "'2000-01-01 00:00:00'",
        "'2030-12-31 23:59:59'",
        "'1700000000'",
    ], "TIME_MODS": [
        "'+1 day'", "'-2 days'", "'+3 hours'", "'-90 minutes'",
        "'start of month'", "'start of year'", "'weekday 0'",
        "'+1 month'", "'-1 year'",
        "'utc'", "'localtime'",
    ], "TIME_FORMATS": [
        "'%Y-%m-%d'", "'%H:%M:%S'", "'%s'", "'%w'", "'%Y %W'", "'%j'", "'%Y-%m-%d %H:%M'"
    ], "DATES": [
        'datetime', 'date', 'time', 'julianday', 'strftime'
    ], "CURRENT": [
        'current_date', 'current_time', 'current_timestamp'
    ]
}

OPS = {
    "INTEGER": ["=", "!=", ">", "<", ">=", "<="],
    "TEXT": ["=", "!="],
    "REAL": ["=", "!=", ">", "<", ">=", "<="],
    "TYPELESS": ["=", "!="],
}