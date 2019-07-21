import constants

TABLE_COLUMNS = {
    constants.TABLENAME_ITEM: [
        "i_id", # integer
        "i_im_id", # integer
        "i_name", # varchar
        "i_price", # float
        "i_data", # varchar
    ],
    constants.TABLENAME_WAREHOUSE: [
        "w_id", # smallint
        "w_name", # varchar
        "w_street_1", # varchar
        "w_street_2", # varchar
        "w_city", # varchar
        "w_state", # varchar
        "w_zip", # varchar
        "w_tax", # float
        "w_ytd", # float
    ],    
    constants.TABLENAME_DISTRICT: [
        "d_id", # tinyint
        "d_w_id", # smallint
        "d_name", # varchar
        "d_street_1", # varchar
        "d_street_2", # varchar
        "d_city", # varchar
        "d_state", # varchar
        "d_zip", # varchar
        "d_tax", # float
        "d_ytd", # float
        "d_next_o_id", # int
    ],
    constants.TABLENAME_CUSTOMER:   [
        "c_id", # integer
        "c_d_id", # tinyint
        "c_w_id", # smallint
        "c_first", # varchar
        "c_middle", # varchar
        "c_last", # varchar
        "c_street_1", # varchar
        "c_street_2", # varchar
        "c_city", # varchar
        "c_state", # varchar
        "c_zip", # varchar
        "c_phone", # varchar
        "c_since", # timestamp
        "c_credit", # varchar
        "c_credit_lim", # float
        "c_discount", # float
        "c_balance", # float
        "c_ytd_payment", # float
        "c_payment_cnt", # integer
        "c_delivery_cnt", # integer
        "c_data", # varchar
    ],
    constants.TABLENAME_STOCK:      [
        "s_i_id", # integer
        "s_w_id", # smallint
        "s_quantity", # integer
        "s_dist_01", # varchar
        "s_dist_02", # varchar
        "s_dist_03", # varchar
        "s_dist_04", # varchar
        "s_dist_05", # varchar
        "s_dist_06", # varchar
        "s_dist_07", # varchar
        "s_dist_08", # varchar
        "s_dist_09", # varchar
        "s_dist_10", # varchar
        "s_ytd", # integer
        "s_order_cnt", # integer
        "s_remote_cnt", # integer
        "s_data", # varchar
    ],
    constants.TABLENAME_ORDERS:     [
        "o_id", # integer
        "o_c_id", # integer
        "o_d_id", # tinyint
        "o_w_id", # smallint
        "o_entry_d", # timestamp
        "o_carrier_id", # integer
        "o_ol_cnt", # integer
        "o_all_local", # integer
    ],
    constants.TABLENAME_NEW_ORDER:  [
        "no_o_id", # integer
        "no_d_id", # tinyint
        "no_w_id", # smallint
    ],
    constants.TABLENAME_ORDER_LINE: [
        "ol_o_id", # integer
        "ol_d_id", # tinyint
        "ol_w_id", # smallint
        "ol_number", # integer
        "ol_i_id", # integer
        "ol_supply_w_id", # smallint
        "ol_delivery_d", # timestamp
        "ol_quantity", # integer
        "ol_amount", # float
        "ol_dist_info", # varchar
    ],
    constants.TABLENAME_HISTORY:    [
        "h_c_id", # integer
        "h_c_d_id", # tinyint
        "h_c_w_id", # smallint
        "h_d_id", # tinyint
        "h_w_id", # smallint
        "h_date", # timestamp
        "h_amount", # float
        "h_data", # varchar
    ],
}
 
