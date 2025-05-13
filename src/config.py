SEED = 42 # random seed
TEST_FOLDER = "data/test/"
QUERY_FOLDER = TEST_FOLDER + "queries/" # save queries from fuzzer
STATS_FOLDER = TEST_FOLDER + "stats/" # fuzzing results statistics
BUGS_FOLDER = TEST_FOLDER + "bugs/" # bugs results
ERROR_FOLDER = TEST_FOLDER + "errors/"
SQLITE_VERSIONS = ["sqlite3-3.26.0", "sqlite3-3.39.4"]

# databases used to test queries
DB = "data/db/test.db"
DB1 = "data/db/test1.db"
DB2 = "data/db/test2.db"

PROB_TABLE2 = {'comp_nullc': 0.8257248848795652, 'comp_callc': 0.8956358573854, 'bet_nullc': 0.4585397572036406, 'bet_callc': 0.6254973648183163, 'like_nullc': 0.2868359237167937, 'like_callc': 0.8389032353547573, 'inli_nullc': 0.4582130559760309, 'inli_callc': 0.4308786382104295, 'where_ex_p': 0.21019945796337475, 'grp_ex_p': 0.25254398797014277, 'ord_ex_p': 0.35085878434590806, '*_ex_p': 0.9266973842162889, 'cols_ex_p': 0.6474511471390385, 'one_ex_p': 0.039885787749804856, 'omit_ex_p': 0.48198316726763835, 'lit_ex_p': 0.25085509203894496, 'nullc': 0.22725110992534975, 'nocol_p': 0.9525475777438619, 'cole_p': 0.009997446745808733, 'lit_p': 0.6318724954961785, 'case_p': 0.7494199369096866, 'time_p': 0.6299978224422471, 'null_p': 0.5731279167600984, 'call_p': 0.012238702159233405, 'one_p': 0.25469612239215544, 'std_p': 0.24699187421090085, 'form_p': 0.7852790552873798, 'cast_p': 0.7607117868048904, 'agg2_p': 0.7318838185260268, 'rexp_p': 0.4374236188226365, 'alias_p': 0.9403536687299024, 'case_col_p': 0.24019152820772183, 'pred_p': 0.33172105461536583, 'depth_p': 0.9109660283753707, 'sub_p': 0.46082706165694387, 'pk_p': 0.6675046842232927, 'unq_p': 0.7906774023981938, 'dft_p': 0.3401297925248757, 'nnl_p': 0.7316630894516405, 'cck_p': 0.8779352961070529, 'typeless_p': 0.09969645515434916, 'full_p': 0.6943805918567315, 'conf_p': 0.9971068502933432, 'where_p': 0.5249774989182139, 'grp_p': 0.8292837675677768, 'ord_p': 0.4812583548372704, 'join_p': 0.21640539108019252, 'lmt_p': 0.13888773173151608, 'offst_p': 0.4082237788894847, '*_p': 0.11585384340524357, 'omit_p': 0.013964833231580757, 'date_p': 0.36704835689279347, 'cols_p': 0.15370757066831997, 'agg_p': 0.6368695669601673, 'count_p': 0.515249162801451, 'rec_p': 0.08563936525534771, 'one_with_p': 0.6328237661619438, '*_with_p': 0.45198838916414563, 'select_p': 0.9884952724388772, 'tmp_p': 0.15933562340416757, 'one_view_p': 0.14105845306711778, 'cols_view_p': 0.10026871987415888, 'alias_view_p': 0.1879489325995085, 'rexp_view_p': 0.04095066450916785, 'uniq_p': 0.9373562050572285, 'rexp_index_p': 0.7985079776507856, 'time_index_p': 0.444905814597926, 'std_index_p': 0.9274163982929028, 'temp_p': 0.41261671367241665, 'nex_p': 0.9441591829046748, 'upcol_p': 0.43630070726128845, 'where_trigger_p': 0.4270948570785992, 'feac_p': 0.34890288643739464, 'dft_trigger_p': 0.2738313249285055, 'rexp_trigger_p': 0.9387971910117401, 'ifex_p': 0.5684285284959675, 'fktbl_p': 0.6799294610595897, 'rollback_p': 0.988035619231176, 'save_p': 0.07724307822439844, 'release_p': 0.4712886062325593, 'table': 0.13149422254121879, 'alt_ren': 0.8919373782880161, 'alt_add': 0.7994694217566117, 'alt_col': 0.742932279435451, 'select1': 0.6978513309149632, 'select2': 0.894338486897019, 'with': 0.5655702413335805, 'view': 0.22471637111294362, 'index': 0.8977732912893189, 'trigger': 0.18569709040017604, 'insert': 0.5367328330021157, 'update': 0.30610069364464537, 'replace': 0.7148586203417445, 'delete': 0.01828870046101102, 'pragma': 0.6431827977012826, 'control': 0.00400021884297229, 'optimize': 0.9966639383404591, 'drop_tbl': 0.6886989889946861}

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
    "fktbl_p":      0.5,
    
    # Transactions
    "rollback_p":   0.0,
    "save_p":       0.25,
    "release_p":    0.5,

    # randomQueryGen
    "table"  :  0.3,
    "alt_ren":  0.2, 
    "alt_add":  0.2,
    "alt_col":  0.2,
    "select1":  0.4,
    "select2":  0.2,
    "with":     0.1,
    "view":     0.1,
    "index":    0.1,
    "trigger":  0.1,
    "insert":   0.2,
    "update":   0.2,
    "replace":  0.2,
    "delete":   0.2,
    "pragma":   0.01,
    "control":  0.01,
    "optimize": 0.01,
    "drop_tbl": 0.05,

}

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