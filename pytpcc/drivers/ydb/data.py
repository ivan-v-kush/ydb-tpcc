import constants


class Warehouse(object):
    __slots__ = (
        'w_id',
        'w_name',
        'w_street_1',
        'w_street_2',
        'w_city',
        'w_state',
        'w_zip',
        'w_tax',
        'w_ytd'
    )

    def __init__(self,  w_id,
                        w_name,
                        w_street_1,
                        w_street_2,
                        w_city,
                        w_state,
                        w_zip,
                        w_tax,
                        w_ytd):

        self.w_id = w_id
        self.w_name = w_name
        self.w_street_1 = w_street_1
        self.w_street_2 = w_street_2
        self.w_city = w_city
        self.w_state = w_state
        self.w_zip = w_zip
        self.w_tax = w_tax
        self.w_tax = w_ytd
   

def prepare_warehouses(tuples):
    return [Warehouse(*data) for data in tuples]


PREPARE = {
    constants.TABLENAME_WAREHOUSE:  prepare_warehouses,   
#    constants.TABLENAME_ITEM:       prepare_item_data,
#    constants.TABLENAME_DISTRICT:   prepare_district_data,
#    constants.TABLENAME_CUSTOMER:   prepare_customer_data,
#    constants.TABLENAME_STOCK:      prepare_stock_data,
#    constants.TABLENAME_ORDERS:     prepare_orders_data,
#    constants.TABLENAME_NEW_ORDER:  prepare_new_order_data,
#    constants.TABLENAME_ORDER_LINE: prepare_order_line_data,
#    constants.TABLENAME_HISTORY:    prepare_history_data,
}

