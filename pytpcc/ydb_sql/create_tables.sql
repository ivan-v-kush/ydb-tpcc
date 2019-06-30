/*
1. all columns in YDB require NULL support (doesn't work NOT NULL) 
2. all tables in YDB require Primary Keys. So table 'history' has it, although in TPC-C doesn't have

*/

CREATE TABLE warehouse (
w_id Int,
w_name String, 
w_street_1 String, 
w_street_2 String, 
w_city String, 
w_state String, 
w_zip String, 
w_tax Decimal(4,2), 
w_ytd Decimal(12,2),
PRIMARY KEY(w_id));

CREATE TABLE district (
d_id Int, 
d_w_id Int,
d_name String, 
d_street_1 String, 
d_street_2 String, 
d_city String, 
d_state String, 
d_zip String, 
d_tax Decimal(4,2), 
d_ytd Decimal(12,2), 
d_next_o_id Int,
PRIMARY KEY(d_id, d_w_id));

CREATE TABLE customer (
c_id Int, 
c_d_id Int,
c_w_id Int, 
c_first String, 
c_middle String, 
c_last String, 
c_street_1 String, 
c_street_2 String, 
c_city String, 
c_state String, 
c_zip String, 
c_phone String, 
c_since Datetime, 
c_credit String, 
c_credit_lim Decimal(12,2), 
c_discount Decimal(4,2), 
c_balance Decimal(12,2), 
c_ytd_payment Decimal(12,2), 
c_payment_cnt Int, 
c_delivery_cnt Int, 
c_data String,
PRIMARY KEY(c_id, c_d_id, c_w_id));

--original doesn't have Primary Key, but YDB requires it--
CREATE TABLE history (
h_c_id Int, 
h_c_d_id Int, 
h_c_w_id Int,
h_d_id Int,
h_w_id Int,
h_date Datetime,
h_amount Decimal(6,2), 
h_data String,
PRIMARY KEY(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id));

CREATE TABLE new_order (
no_o_id Int,
no_d_id Int,
no_w_id Int,
PRIMARY KEY(no_o_id, no_d_id, no_w_id));

CREATE TABLE orders (
o_id Int, 
o_d_id Int, 
o_w_id Int,
o_c_id Int,
o_entry_d Datetime,
o_carrier_id Int,
o_ol_cnt Int, 
o_all_local Int,
PRIMARY KEY(o_id, o_d_id, o_w_id));

CREATE TABLE order_line ( 
ol_o_id Int, 
ol_d_id Int,
ol_w_id Int,
ol_number Int,
ol_i_id Int, 
ol_supply_w_id Int,
ol_delivery_d Datetime, 
ol_quantity Int, 
ol_amount Decimal(22,9), 
ol_dist_info String,
PRIMARY KEY(ol_o_id, ol_d_id, ol_w_id, ol_number));

CREATE TABLE item (
i_id Int, 
i_im_id Int, 
i_name String, 
i_price Decimal(5,2), 
i_data String,
PRIMARY KEY(i_id));

CREATE TABLE stock (
s_i_id Int, 
s_w_id Int, 
s_quantity Int, 
s_dist_01 String, 
s_dist_02 String,
s_dist_03 String,
s_dist_04 String, 
s_dist_05 String, 
s_dist_06 String, 
s_dist_07 String, 
s_dist_08 String, 
s_dist_09 String, 
s_dist_10 String, 
s_ytd Int, 
s_order_cnt Int,
s_remote_cnt Int,
s_data String,
PRIMARY KEY(s_i_id, s_w_id));
