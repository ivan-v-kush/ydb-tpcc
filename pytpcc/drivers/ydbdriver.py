from __future__ import with_statement

import os
from kikimr.public.sdk.python import client as ydb
#from python import client as ydb
import logging
#import commands
from pprint import pprint,pformat

import constants
from abstractdriver import *
import pdb

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

TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = ? AND no_w_id = ? AND no_o_id > -1 LIMIT 1", #
        "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
        "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
        "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
    },
    
    "NEW_ORDER" : {
        "getWarehouseTaxRate": "SELECT w_tax FROM warehouse WHERE w_id = $w_id", # w_id
        "getDistrict": "SELECT d_tax, d_next_o_id FROM district WHERE d_id = $d_id AND d_w_id = $w_id", # d_id, w_id
        "getCustomer": "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = $w_id AND c_d_id = $d_id AND c_id = $c_id", # w_id, d_id, c_id
        
        "createOrder": "INSERT INTO order (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES ($d_next_o_id, $d_id, $w_id, $c_id, $o_entry_d, $o_carrier_id, $o_ol_cnt, $o_all_local)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ($o_id, $d_id, $w_id)", # o_id, d_id, w_id
        "incrementNextOrderId": "UPDATE district SET d_next_o_id = d_next_o_id+1 WHERE d_id = $d_id AND d_w_id = $w_id;", # d_next_o_id, d_id, w_id
        
        "getItemInfo": "SELECT i_price, i_name, i_data FROM item WHERE i_id = $ol_i_id", # ol_i_id
        "getStockInfo": "SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt, s_dist_%02d FROM stock WHERE s_i_id = ? AND s_w_id = ?", # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? where s_i_id = ? and s_w_id = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        
    },
    
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id        
    },
    
    "PAYMENT": {
        "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
        "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", # h_amount, w_id
        "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
        "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?", # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    },
    
    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", 
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = ?
              AND OL_D_ID = ?
              AND OL_O_ID < ?
              AND OL_O_ID >= ?
              AND S_W_ID = ?
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < ?
        """,
    },
}



def is_directory_exists(driver, path):
    try:
        return driver.scheme_client.describe_path(path).is_directory()
    except ydb.SchemeError:
        return False


def ensure_path_exists(driver, database, path):
    paths_to_create = list()
    path = path.rstrip("/")
    while path not in ("", database):
        full_path = os.path.join(database, path)
        if is_directory_exists(driver, full_path):
            break
        paths_to_create.append(full_path)
        path = os.path.dirname(path).rstrip("/")

    while len(paths_to_create) > 0:
        full_path = paths_to_create.pop(-1)
        driver.scheme_client.make_directory(full_path)
        

def credentials_from_environ():
    # dynamically import required authentication libraries
    if os.getenv('YDB_TOKEN') is not None:
        return ydb.AuthTokenCredentials(os.getenv('YDB_TOKEN'))

    if os.getenv('SA_ID') is not None:
        with open(os.getenv('SA_PRIVATE_KEY_FILE')) as private_key_file:
            from kikimr.public.sdk.python import iam
            root_certificates_file = os.getenv('SSL_ROOT_CERTIFICATES_FILE',  None)
            iam_channel_credentials = {}
            if root_certificates_file is not None:
                with open(root_certificates_file, 'rb') as root_certificates_file:
                    root_certificates = root_certificates_file.read()
                iam_channel_credentials = {'root_certificates': root_certificates}
            return iam.ServiceAccountCredentials(
                iam_endpoint=os.getenv('IAM_ENDPOINT', 'iam.api.cloud.yandex.net:443'),
                iam_channel_credentials=iam_channel_credentials,
                access_key_id=os.getenv('SA_ACCESS_KEY_ID'),
                service_account_id=os.getenv('SA_ID'),
                private_key=private_key_file.read()
            )
    return None


def describe_table(session, path, name):
    result = session.describe_table(os.path.join(path, name))
    print("\n> describe table: {}".format(name))
    for column in result.columns:
        print("column, name:", column.name, ",", str(column.type.item).strip())
        
def prepare_type_for_ydb(x):
    if x is None:
        return 'null'
    
    x_type = type(x)
    if x_type is int:
        return str(x)
    elif x_type is datetime:
        x = x.strftime("%Y-%m-%dT%H:%M:%SZ")
        return "CAST('{}' AS Datetime)".format(x)
    else:
        return "'" + str(x) + "'"

import iso8601


def to_days(date):
    timedelta = iso8601.parse_date(date) - iso8601.parse_date("1970-1-1")
    return timedelta.days

## ==============================================
## YdbDriver
## ==============================================
class YdbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "endpoint": ("Host", "ydb-ru.yandex.net:2135" ),
        "database": ("The path to the YDB database", "/ru/home/ivan-kush/mydb" ),
        "path": ("Path", "" )
    }
    
    def __init__(self, ddl):
        super(YdbDriver, self).__init__("ydb", ddl)
        self.database = None
        self.conn = None
        self.cursor = None
    
    def __del__(self):
        self.driver.stop
        
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return YdbDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in YdbDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.endpoint = str(config["endpoint"])
        self.database = str(config["database"])
        self.path = str(config["path"])
        
        # TODO check exist db
        # if config["reset"] and os.path.exists(self.database):
        #    logging.debug("Deleting database '%s'" % self.database)
        #    os.unlink(self.database)
        
        #if os.path.exists(self.database) == False:
        #    logging.debug("Loading DDL file '%s'" % (self.ddl))
        #    ## HACK
        #    cmd = "sqlite3 %s < %s" % (self.database, self.ddl)
        #    (result, output) = commands.getstatusoutput(cmd)
        #    assert result == 0, cmd + "\n" + output
        ### IF
        driver_config = ydb.DriverConfig(
            self.endpoint, database=self.database, credentials=credentials_from_environ())
        try:
            self.driver = ydb.Driver(driver_config)
            self.driver.wait(timeout=5)
        except TimeoutError:
            raise RuntimeError("Connect failed to YDB")
    
       # try:
        self.session = self.driver.table_client.session().create()
        ensure_path_exists(self.driver, self.database, self.path)
        
        with open(self.ddl, 'r') as file_schema:
            schema = file_schema.read()
            #schema = 'PRAGMA TablePathPrefix("{}");\n' + schema
            self.session.execute_scheme(schema.format(self.database))
        describe_table(self.session, self.database, "item")
    
    
    #TODO loadFinishItem для информирования о загрузке?
    
    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------

    def loadTuples(self, tableName, tuples):
        len_tuples = len(tuples)
        logging.info("Data count: {}".format(len_tuples))
        if len(tuples) == 0: return
        
        #TODO 1-ой командой? self.cursor.executemany(sql, tuples)
        columns = TABLE_COLUMNS[tableName]
        columns_str = ",".join(columns) 
        
        sql="""PRAGMA TablePathPrefix("{}");
            INSERT INTO {} ({})
            VALUES ({});
            """
        
        #TODO
        #gg="""PRAGMA TablePathPrefix("{}");
        #    DECLARE $a AS Int;
        #    DECLARE $b AS Int;
        #    DECLARE $c AS Int;
        #    DECLARE $c_sin AS String;
        #    INSERT INTO customer (c_id, c_d_id, c_w_id, c_since)
        #    VALUES ($a, $b, $c, CAST($c_sin AS Datetime));
        #    """

        for data in tuples:
            #pdb.set_trace()
            data = list(map(( lambda x: prepare_type_for_ydb(x)), data))
            data_str = ",".join(map(str, data))
            # logging.debug("Data str: {}".format(data_str))
            #query = sql.format(self.database)
            query = sql.format(self.database, tableName, columns_str, data_str)
            prepared_query=self.session.prepare(query)
            self.session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query,
                {},
                #{'$a': data[0], 
                # '$b': data[1], 
                # '$c': data[2],
                # '$c_sin': str.encode(data[12])},
                commit_tx=True
            )
        logging.debug("Losecondary_index/app/repository.pyaded %d tuples for tableName %s" % (len(tuples), tableName))
        return
    
    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def TODOloadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        
        #TODO 1-ой командой? self.cursor.executemany(sql, tuples)
        columns = TABLE_COLUMNS[tableName]
        subs = list(map(( lambda x: '$' + x), columns))
        
        columns_str = ",".join(columns) 
        subs_str = ",".join(subs) 
        sql = "INSERT INTO {} ({}) VALUES ({})".format(tableName, columns_str, subs_str)

        for data in tuples:
            self.session.transaction(ydb.SerializableReadWrite()).execute(
                sql, {
                    '$seriesId': series_id,
                    '$seasonId': season_id,
                    '$episodeId': episode_id,
                },
                commit_tx=True
            )
        
        logging.debug("Losecondary_index/app/repository.pyaded %d tuples for tableName %s" % (len(tuples), tableName))
        return

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    #def loadFinish(self):
    #    logging.info("Commiting changes to database")
        #TODO maybe here call commit?  Not immediately? 
        #TODO Nope
        # self.conn.commit()

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        q = TXN_QUERIES["DELIVERY"]
        
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            self.cursor.execute(q["getNewOrder"], [d_id, w_id])
            newOrder = self.cursor.fetchone()
            if newOrder == None:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            assert len(newOrder) > 0
            no_o_id = newOrder[0]
            
            self.cursor.execute(q["getCId"], [no_o_id, d_id, w_id])
            c_id = self.cursor.fetchone()[0]
            
            self.cursor.execute(q["sumOLAmount"], [no_o_id, d_id, w_id])
            ol_total = self.cursor.fetchone()[0]

            self.cursor.execute(q["deleteNewOrder"], [d_id, w_id, no_o_id])
            self.cursor.execute(q["updateOrders"], [o_carrier_id, no_o_id, d_id, w_id])
            self.cursor.execute(q["updateOrderLine"], [ol_delivery_d, no_o_id, d_id, w_id])

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            self.cursor.execute(q["updateCustomer"], [ol_total, c_id, d_id, w_id])

            result.append((d_id, no_o_id))
        ## FOR

        self.conn.commit()
        return result

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        q = TXN_QUERIES["NEW_ORDER"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True
        items = [ ]
        for i in range(len(i_ids)):
            ## Determine if this is an all local order or not
            all_local = all_local and i_w_ids[i] == w_id
            self.cursor.execute(q["getItemInfo"], [i_ids[i]])
            items.append(self.cursor.fetchone())
        assert len(items) == len(i_ids)
        
        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        for item in items:
            if len(item) == 0:
                ## TODO Abort here!
                return
        ## FOR
        
        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        self.cursor.execute(q["getWarehouseTaxRate"], [w_id])
        w_tax = self.cursor.fetchone()[0]
        
        self.cursor.execute(q["getDistrict"], [d_id, w_id])
        district_info = self.cursor.fetchone()
        d_tax = district_info[0]
        d_next_o_id = district_info[1]
        
        self.cursor.execute(q["getCustomer"], [w_id, d_id, c_id])
        customer_info = self.cursor.fetchone()
        c_discount = customer_info[0]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        
        self.cursor.execute(q["incrementNextOrderId"], [d_next_o_id + 1, d_id, w_id])
        self.cursor.execute(q["createOrder"], [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local])
        self.cursor.execute(q["createNewOrder"], [d_next_o_id, d_id, w_id])

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        item_data = [ ]
        total = 0
        for i in range(len(i_ids)):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            itemInfo = items[i]
            i_name = itemInfo[1]
            i_data = itemInfo[2]
            i_price = itemInfo[0]

            self.cursor.execute(q["getStockInfo"] % (d_id), [ol_i_id, ol_supply_w_id])
            stockInfo = self.cursor.fetchone()
            if len(stockInfo) == 0:
                logging.warn("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
                continue
            s_quantity = stockInfo[0]
            s_ytd = stockInfo[2]
            s_order_cnt = stockInfo[3]
            s_remote_cnt = stockInfo[4]
            s_data = stockInfo[1]
            s_dist_xx = stockInfo[5] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            
            if ol_supply_w_id != w_id: s_remote_cnt += 1

            self.cursor.execute(q["updateStock"], [s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id])

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            self.cursor.execute(q["createOrderLine"], [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx])

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR
        
        ## Commit!
        self.conn.commit()

        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        
        return [ customer_info, misc, item_data ]

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = TXN_QUERIES["ORDER_STATUS"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1)/2
            customer = all_customers[index]
            c_id = customer[0]
        assert len(customer) > 0
        assert c_id != None

        self.cursor.execute(q["getLastOrder"], [w_id, d_id, c_id])
        order = self.cursor.fetchone()
        if order:
            self.cursor.execute(q["getOrderLines"], [w_id, d_id, order[0]])
            orderLines = self.cursor.fetchall()
        else:
            orderLines = [ ]

        self.conn.commit()
        return [ customer, order, orderLines ]

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        q = TXN_QUERIES["PAYMENT"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1)/2
            customer = all_customers[index]
            c_id = customer[0]
        assert len(customer) > 0
        c_balance = customer[14] - h_amount
        c_ytd_payment = customer[15] + h_amount
        c_payment_cnt = customer[16] + 1
        c_data = customer[17]

        self.cursor.execute(q["getWarehouse"], [w_id])
        warehouse = self.cursor.fetchone()
        
        self.cursor.execute(q["getDistrict"], [w_id, d_id])
        district = self.cursor.fetchone()
        
        self.cursor.execute(q["updateWarehouseBalance"], [h_amount, w_id])
        self.cursor.execute(q["updateDistrictBalance"], [h_amount, w_id, d_id])

        # Customer Credit Information
        if customer[11] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            self.cursor.execute(q["updateBCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id])
        else:
            c_data = ""
            self.cursor.execute(q["updateGCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id])

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (warehouse[0], district[0])
        # Create the history record
        self.cursor.execute(q["insertHistory"], [c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data])

        self.conn.commit()

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return [ warehouse, district, customer ]
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        q = TXN_QUERIES["STOCK_LEVEL"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        self.cursor.execute(q["getOId"], [w_id, d_id])
        result = self.cursor.fetchone()
        assert result
        o_id = result[0]
        
        self.cursor.execute(q["getStockCount"], [w_id, d_id, o_id, (o_id - 20), w_id, threshold])
        result = self.cursor.fetchone()
        
        self.conn.commit()
        
        return int(result[0])
        
## CLASS
