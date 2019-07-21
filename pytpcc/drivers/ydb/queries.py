import constants


class FILL_QUERIES:

    warehouse = """
        DECLARE $warehouseData AS "List<Struct<
        w_id Int,
        w_name String, 
        w_street_1 String, 
        w_street_2 String, 
        w_city String, 
        w_state String, 
        w_zip String, 
        w_tax Decimal(4,2), 
        w_ytd Decimal(12,2)
        >>";
        
        REPLACE INTO warehouse
        SELECT
            w_id,
            w_name,
            w_street_1,
            w_street_2,
            w_city,
            w_state,
            w_zip,
            w_tax,
            w_ytd
        FROM AS_TABLE($warehouseData);
        """ # w_id


FILL = {
    constants.TABLENAME_WAREHOUSE:  FILL_QUERIES.warehouse,   
#    constants.TABLENAME_ITEM:       prepare_item_data,
#    constants.TABLENAME_DISTRICT:   prepare_district_data,
#    constants.TABLENAME_CUSTOMER:   prepare_customer_data,
#    constants.TABLENAME_STOCK:      prepare_stock_data,
#    constants.TABLENAME_ORDERS:     prepare_orders_data,
#    constants.TABLENAME_NEW_ORDER:  prepare_new_order_data,
#    constants.TABLENAME_ORDER_LINE: prepare_order_line_data,
#    constants.TABLENAME_HISTORY:    prepare_history_data,
}


FILL_VAR = {
    constants.TABLENAME_WAREHOUSE:  '$warehouseData',   
#    constants.TABLENAME_ITEM:       prepare_item_data,
#    constants.TABLENAME_DISTRICT:   prepare_district_data,
#    constants.TABLENAME_CUSTOMER:   prepare_customer_data,
#    constants.TABLENAME_STOCK:      prepare_stock_data,
#    constants.TABLENAME_ORDERS:     prepare_orders_data,
#    constants.TABLENAME_NEW_ORDER:  prepare_new_order_data,
#    constants.TABLENAME_ORDER_LINE: prepare_order_line_data,
#    constants.TABLENAME_HISTORY:    prepare_history_data,
}

class NEW_ORDER:
    
    getWarehouseTaxRate = """
        DECLARE $w_id AS Int;
        
        SELECT w_tax FROM warehouse
        WHERE w_id = $w_id;
        """ # w_id

    getDistrict = """
        DECLARE $d_w_id AS Int;
        DECLARE $d_id AS Int;
        
        SELECT d_tax, d_next_o_id
        FROM district
        WHERE d_id = $d_id AND d_w_id = $d_w_id;
        """ # d_id, w_id
        
    getCustomer = """
        DECLARE $c_w_id AS Int;
        DECLARE $c_d_id AS Int;
        DECLARE $c_id AS Int;
        
        SELECT c_discount, c_last, c_credit 
        FROM customer 
        WHERE c_w_id = $c_w_id 
            AND c_d_id = $c_d_id 
            AND c_id = $c_id;
        """ # w_id, d_id, c_id
    
    createOrder = """
        DECLARE $o_id AS Int;
        DECLARE $o_d_id AS Int;
        DECLARE $o_w_id AS Int;
        DECLARE $o_c_id AS Int;
        DECLARE $o_entry_d AS String;
        DECLARE $o_carrier_id AS Int;
        DECLARE $o_ol_cnt AS Int;
        DECLARE $o_all_local AS Int;

        INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, 
            o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) 
        VALUES ($o_id, $o_d_id, $o_w_id, $o_c_id, 
            CAST($o_entry_d AS Datetime), $o_carrier_id, $o_ol_cnt, $o_all_local);
        """ # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
    
    createNewOrder = """
        DECLARE $no_o_id AS Int;
        DECLARE $no_d_id AS Int;
        DECLARE $no_w_id AS Int;
        
        INSERT INTO new_order (no_o_id, no_d_id, no_w_id) 
        VALUES ($no_o_id, $no_d_id, $no_w_id)
        """ # o_id, d_id, w_id

    incrementNextOrderId = """
        DECLARE $d_id AS Int;
        DECLARE $d_w_id AS Int;
    
        UPDATE district 
        SET d_next_o_id = d_next_o_id + 1 
        WHERE d_id = $d_id AND d_w_id = $d_w_id;
        """ # d_next_o_id, d_id, w_id
        
    getItemInfo= """
        DECLARE $i_id AS Int;
        
        SELECT i_price, i_name, i_data 
        FROM item 
        WHERE i_id = $i_id;
        """ # ol_i_id
    
    getStockInfo = """
        DECLARE $s_i_id AS Int;
        DECLARE $s_w_id AS Int;
        
        SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt, s_dist_{:02d}
        FROM stock 
        WHERE s_i_id = $s_i_id AND s_w_id = $s_w_id;
        """ # d_id, ol_i_id, ol_supply_w_id

    updateStock = """
        DECLARE $s_i_id AS Int;
        DECLARE $s_w_id AS Int;
        DECLARE $s_quantity AS Int;
        DECLARE $s_ytd AS Int;
        DECLARE $s_order_cnt AS Int;
        DECLARE $s_remote_cnt AS Int;

        $to_update = (
            SELECT s_i_id, s_w_id, $s_quantity AS s_quantity, $s_ytd AS s_ytd, $s_order_cnt AS s_order_cnt, $s_remote_cnt AS s_remote_cnt
            FROM stock
            WHERE s_i_id = $s_i_id AND s_w_id = $s_w_id
        );
        
        UPDATE stock ON
        SELECT * FROM $to_update;
        """ # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id

    createOrderLine = """
        DECLARE $ol_o_id AS Int;
        DECLARE $ol_d_id AS Int;
        DECLARE $ol_w_id AS Int;
        DECLARE $ol_number AS Int;
        DECLARE $ol_i_id AS Int;
        DECLARE $ol_supply_w_id AS Int;
        DECLARE $ol_delivery_d AS String;
        DECLARE $ol_quantity AS Int;
        DECLARE $ol_amount AS Decimal(22,9);
        DECLARE $ol_dist_info AS String;

        UPSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, 
                                ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) 
        VALUES ($ol_o_id, $ol_d_id, $ol_w_id, $ol_number, $ol_i_id, $ol_supply_w_id, 
                CAST($ol_delivery_d AS Datetime), $ol_quantity, $ol_amount, $ol_dist_info);
        """ # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        


class DELIVERY:
    getNewOrder = """
        DECLARE $no_d_id AS Int;
        DECLARE $no_w_id AS Int;

        SELECT no_o_id 
        FROM new_order 
        WHERE no_d_id = $no_d_id 
            AND no_w_id = $no_w_id 
            AND no_o_id > -1 LIMIT 1;
        """ #

    deleteNewOrder = """
        DECLARE $no_d_id AS Int;
        DECLARE $no_w_id AS Int;
        DECLARE $no_o_id AS Int;

        DELETE FROM new_order 
        WHERE no_d_id = $no_d_id 
            AND no_w_id = $no_w_id 
            AND no_o_id = $no_o_id;
        """ # d_id, w_id, no_o_id

    getCId = """
        DECLARE $o_id AS Int;
        DECLARE $o_w_id AS Int;
        DECLARE $o_d_id AS Int;
        
        SELECT o_c_id 
        FROM orders 
        WHERE o_id = $o_id 
            AND o_d_id = $o_d_id 
            AND o_w_id = $o_w_id;
        """ # no_o_id, d_id, w_id

    updateOrders = """
        DECLARE $o_id AS Int;
        DECLARE $o_w_id AS Int;
        DECLARE $o_d_id AS Int;
        DECLARE $o_carrier_id AS Int;
        
        UPDATE orders 
        SET o_carrier_id = $o_carrier_id 
        WHERE o_id = $o_id
            AND o_d_id = $o_d_id
            AND o_w_id = $o_w_id;
        """ # o_carrier_id, no_o_id, d_id, w_id

    updateOrderLine = """
        DECLARE $o_id AS Int;
        DECLARE $o_w_id AS Int;
        DECLARE $o_d_id AS Int;
        DECLARE $ol_delivery_d AS String;
        
        UPDATE order_line 
        SET ol_delivery_d =  CAST($ol_delivery_d AS Datetime)
        WHERE ol_o_id = $ol_o_id
            AND ol_d_id = $ol_d_id 
            AND ol_w_id = $ol_w_id;
        """ # o_entry_d, no_o_id, d_id, w_id

    sumOLAmount = """
        DECLARE $ol_o_id AS Int;
        DECLARE $o_w_id AS Int;
        DECLARE $o_d_id AS Int;
        
        SELECT SUM(ol_amount) 
        FROM order_line
        WHERE ol_o_id = $ol_o_id
            AND ol_d_id = $ol_d_id 
            AND ol_w_id = $ol_w_id;
        """ # no_o_id, d_id, w_id

    updateCustomer = """
        DECLARE $c_id AS Int;
        DECLARE $c_d_id AS Int;
        DECLARE $c_w_id AS Int;
        DECLARE $delta_balance AS Decimal(12,2);
        
        UPDATE customer 
        SET c_balance = c_balance + $delta_balance
        WHERE c_id = $c_id
            AND c_d_id = $c_d_id
            AND c_w_id = $c_w_id;
        """ # ol_total, c_id, d_id, w_id


class ORDER_STATUS:

    getCustomerByCustomerId = """
        DECLARE $c_id AS Int;
        DECLARE $c_d_id AS Int;
        DECLARE $c_w_id AS Int;
        
        SELECT c_id, c_first, c_middle, c_last, c_balance 
        FROM customer 
        WHERE c_w_id = $c_w_id
            AND c_d_id = $c_d_id
            AND c_id = $c_id;
        """ # w_id, d_id, c_id

    getCustomersByLastName = """
        DECLARE $c_last AS String;
        DECLARE $c_d_id AS Int;
        DECLARE $c_w_id AS Int;
        
        SELECT c_id, c_first, c_middle, c_last, c_balance 
        FROM customer 
        WHERE c_w_id = $c_w_id
            AND c_d_id = $c_d_id
            AND c_last = $c_last
        ORDER BY c_first;
        """ # w_id, d_id, c_last

    getLastOrder = """
        DECLARE $o_w_id AS Int;
        DECLARE $o_d_id AS Int;
        DECLARE $o_c_id AS Int;
        
        SELECT o_id, o_carrier_id, o_entry_d 
        FROM orders 
        WHERE o_w_id = $o_w_id
            AND o_d_id = $o_d_id
            AND o_c_id = $o_c_id
        ORDER BY o_id DESC LIMIT 1;
        """ # w_id, d_id, c_id

    getOrderLines = """
        DECLARE $ol_w_id AS Int;
        DECLARE $ol_d_id AS Int;
        DECLARE $ol_o_id AS Int;
        
        SELECT ol_supply_w_id, ol_i_id, ol_quantity, ol_amount, ol_delivery_d 
        FROM order_line 
        WHERE ol_w_id = $ol_w_id 
            AND ol_d_id = $ol_d_id 
            AND ol_o_id = $ol_o_id;
        """ # w_id, d_id, o_id        


class PAYMENT:
    getWarehouse = """
        DECLARE $w_id AS Int;

        SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip 
        FROM warehouse
        WHERE w_id = $w_id;
        """ # w_id

    updateWarehouseBalance = """
        DECLARE $w_id AS Int;
        DECLARE $delta_w_ytd AS Decimal(12,2);
        
        UPDATE warehouse
        SET w_ytd = w_ytd + $delta_w_ytd
        WHERE w_id = $w_id;
        """ # h_amount, w_id

    getDistrict = """
        DECLARE $d_id AS Int;
        DECLARE $d_w_id AS Int;
    
        SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip 
        FROM district
        WHERE d_w_id = $d_w_id AND d_id = $d_id;
        """ # w_id, d_id

    updateDistrictBalance = """
        DECLARE $delta_d_ytd AS Decimal(12,2);
        DECLARE $d_id AS Int;
        DECLARE $d_w_id AS Int;
    
        UPDATE district SET d_ytd = d_ytd + $delta_d_ytd
        WHERE d_w_id = $d_w_id AND d_id = $d_id;
        """ # h_amount, d_w_id, d_id

    getCustomerByCustomerId = """
        DECLARE $c_id AS Int;
        DECLARE $c_d_id AS Int;
        DECLARE $c_w_id AS Int;

        SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, 
            c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data 
        FROM customer 
        WHERE c_w_id = $c_w_id
            AND c_d_id = $c_d_id
            AND c_id = $c_id;
        """ # w_id, d_id, c_id

    getCustomersByLastName = """
        DECLARE $c_w_id AS Int;
        DECLARE $c_d_id AS Int;
        DECLARE $c_last AS String;
        
        SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, 
            c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data 
        FROM customer 
        WHERE c_w_id = $c_w_id
            AND c_d_id = $c_d_id 
            AND c_last = $c_last
        ORDER BY c_first;
        """ # w_id, d_id, c_last

    updateBCCustomer = """
        DECLARE $c_data AS Int;
        DECLARE $c_balance AS Decimal(12, 2);
        DECLARE $c_ytd_payment AS Decimal(12, 2);
        DECLARE $c_payment_cnt AS Int;    
        DECLARE $c_w_id AS Int;
        DECLARE $c_d_id AS Int;
        DECLARE $c_id AS Int;

        UPDATE customer 
        SET c_balance = $c_balance, 
            c_ytd_payment = $c_ytd_payment, 
            c_payment_cnt = $c_payment_cnt, 
            c_data = $c_data 
        WHERE c_w_id = $c_w_id
            AND c_d_id = $c_d_id
            AND c_id = $c_id;
        """ # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id

    updateGCCustomer = """
        DECLARE $c_balance AS Decimal(12, 2);
        DECLARE $c_ytd_payment AS Decimal(12, 2);
        DECLARE $c_payment_cnt AS Int;    
        DECLARE $c_w_id AS Int;
        DECLARE $c_d_id AS Int;
        DECLARE $c_id AS Int;

        UPDATE customer 
        SET c_balance = $c_balance,
            c_ytd_payment = $c_ytd_payment,
            c_payment_cnt = $c_payment_cnt
        WHERE c_w_id = $c_w_id
            AND c_d_id = $c_d_id
            AND c_id = $c_id;
        """ # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id

    insertHistory = """
        DECLARE $h_c_id AS Int; 
        DECLARE $h_c_d_id AS Int; 
        DECLARE $h_c_w_id AS Int;
        DECLARE $h_d_id AS Int;
        DECLARE $h_w_id AS Int;
        DECLARE $h_date String;
        DECLARE $h_amount Decimal(6,2); 
        DECLARE $h_data String;

        INSERT INTO history 
        VALUES ($h_c_id, $h_c_d_id, $h_c_w_id, $h_d_id, $h_w_id, CADT($h_date AS Datetime), $h_amount, $h_data);
        """


class STOCK_LEVEL:
    
    getOId = """
        DECLARE $d_w_id AS Int;
        DECLARE $d_id AS Int;
        
        SELECT d_next_o_id 
        FROM district 
        WHERE d_w_id = $d_w_id AND d_id = $d_id;
        """
    
    getStockCount = """
        DECLARE $ol_w_id AS Int;
        DECLARE $ol_d_id AS Int;
        DECLARE $ol_o_id_max AS Int;
        DECLARE $ol_o_id_min AS Int;
        DECLARE $s_w_id AS Int;;
        DECLARE $s_quantity_max AS Int;

        SELECT COUNT(DISTINCT(ol_i_id)) 
        FROM order_line, stock
        WHERE ol_w_id = $ol_w_id
            AND ol_d_id = $ol_d_id
            AND ol_o_id < $ol_o_id_max
            AND ol_o_id >= $ol_o_id_min
            AND s_w_id = $s_w_id
            AND s_i_id = ol_i_id
            AND s_quantity < $s_quantity_max;
        """ 
