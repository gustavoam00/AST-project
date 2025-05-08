SEED = 42 # random seed
TEST_FOLDER = "test/" # folder to save bugs and queries
COMPACT = 5000 # compacting query length

PROB_TABLE = {
    "comp_nullc": 0.05, "comp_callc": 0.9,
    "bet_nullc" : 0, "bet_callc" : 0.9,
    "like_nullc" : 0.05, "like_callc" : 0.9,
    "inli_nullc" : 0.05, "inli_callc" : 0.9,
    "where_p" : 1, "grp_p" : 0, "ord_p" : 0,
    "nullc" : 0.5,
    "pred_p":1.0, "depth_p":0.7, "sub_p":0.05, "where_p":0.05,
    "pk_p":0.0, "unq_p":0.05, "dft_p":0.2, "nnl_p":0.2, "cck_p":0.3, "typeless_p":0.1,
    "conf_p":0.2, "null_p":0.05, "call_p":0.9, "full_p": 0.2,
    "join_p":0.3, "lmt_p":0.2, "case_p":0.2, 
    "offst_p":0.5, "*_p":0.2, "omit_p":0.2, "one_p":0.1,
    "urec_p": 0.5, "uniq_p": 0.01,
    "temp_p":0.2, "nex_p":0.2, "upcol_p":0.2, "feac_p":0.2,
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