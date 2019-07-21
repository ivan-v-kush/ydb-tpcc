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

import ydb.queries as queries
from ydb.columns import *
import ydb.data as ydb_data

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

def prepare_datetime(d):
    d_tmp = d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str.encode(d_tmp)

## ==============================================
## YdbDriver
## ==============================================
class YdbDriver(AbstractDriver):
    def prepareQuery(self, query, *args):
        corrected = 'PRAGMA TablePathPrefix("{}");\n' + query
        return self.session.prepare(corrected.format(self.database, *args))

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

        for data in tuples:
            data = list(map(( lambda x: prepare_type_for_ydb(x)), data))
            data_str = ",".join(map(str, data))
            # logging.debug("Data str: {}".format(data_str))
            #query = sql.format(self.database)
            query = sql.format(self.database, tableName, columns_str, data_str)
            prepared_query = self.session.prepare(query)
            self.session.transaction(ydb.SerializableReadWrite()).execute(
                prepared_query,
                {},
                #{'$a': data[0], 
                # '$b': data[1], 
                # '$c': data[2],
                # '$c_sin': str.encode(data[12])},
                commit_tx=True
            )
        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        return
    
    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def TODOloadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        
        data = ydb_data.PREPARE[tableName](tuples)
        query = queries.FILL[tableName]
        var = queries.FILL_VAR[tableName]

        prepared_query = self.prepareQuery(query)
        session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query,
            commit_tx=True,
            parameters={
                var: data
            }
        )
        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
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
        q = queries.DELIVERY
        
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = [ ]
        tx = self.session.transaction(ydb.SerializableReadWrite()).begin()

        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            prepared_query = self.prepareQuery(q.getNewOrder)
            newOrderInfo = tx.execute(
                prepared_query, {
                    '$no_w_id': w_id,
                    '$no_d_id': d_id,
                }
            )
            newOrderInfo = newOrderInfo[0].rows[0]
            #TODO проверки на отсутствие записей?
            if newOrderInfo == None:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            #TODO надо?
            assert len(newOrderInfo) > 0
            no_o_id = newOrderInfo['no_o_id']
            
            prepared_query = self.prepareQuery(q.getCId)
            orderInfo = tx.execute(
                prepared_query, {
                    '$o_id': no_o_id,
                    '$o_w_id': w_id,
                    '$o_d_id': d_id,
                }
            )
            orderInfo = orderInfo[0].rows[0]
            c_id = orderInfo['o_c_id']
            
            prepared_query = self.prepareQuery(q.sumOLAmount)
            orderLineInfo = tx.execute(
                prepared_query, {
                    '$ol_o_id': no_o_id,
                    '$ol_w_id': w_id,
                    '$ol_d_id': d_id,
                }
            )
            orderLineInfo = orderLineInfo[0].rows[0]
            ol_total = orderLineInfo['sum_ol_total'] #TODO sum?

            prepared_query = self.prepareQuery(q.deleteNewOrder)
            tx.execute(
                prepared_query, {
                    '$no_d_id': no_o_id,
                    '$no_w_id': w_id,
                    '$no_d_id': d_id,
                }
            )
            
            prepared_query = self.prepareQuery(q.updateOrders)
            tx.execute(
                prepared_query, {
                    '$o_d_id': no_o_id,
                    '$o_w_id': w_id,
                    '$o_d_id': d_id,
                }
            )

            prepared_query = self.prepareQuery(q.updateOrderLine)
            tx.execute(
                prepared_query, {
                    '$ol_delivery_d': prepare_datetime(ol_delivery_d) #TODO to var?
                    '$ol_d_id': no_o_id,
                    '$ol_w_id': w_id,
                    '$ol_d_id': d_id,
                }
            )

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            prepared_query = self.prepareQuery(q.updateCustomer)
            tx.execute(
                prepared_query, {
                    '$delta_balance': ol_total
                    '$c_id': c_id,
                    '$c_w_id': w_id,
                    '$c_d_id': d_id,
                }
            )

            result.append((d_id, no_o_id))
        ## FOR

        tx.commit()
        #TODO returns
        return result

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        q = queries.NEW_ORDER
        
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
        results = []
        
        tx = self.session.transaction(ydb.SerializableReadWrite()).begin()
        
        for i in range(len(i_ids)):
            ## Determine if this is an all local order or not
            all_local = all_local and i_w_ids[i] == w_id
            
            prepared_query = self.prepareQuery(q.getItemInfo)
            result = tx.execute(
                prepared_query, {
                    '$i_id': i_ids[i]
                }
            )
            results.append(result[0])
        items = [ ]
        for result in results:
            for row in result.rows:
                items.append(row)
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
        prepared_query = self.prepareQuery(q.getWarehouseTaxRate)
        tax_info = tx.execute(
            prepared_query, {
                '$w_id': w_id
            }
        )
        w_tax = tax_info[0].rows[0]['w_tax']

        prepared_query = self.prepareQuery(q.getDistrict)
        district_info = tx.execute(
                prepared_query, {
                    '$d_w_id': w_id,
                    '$d_id': d_id,
                }
        )
        district_info = district_info[0].rows[0]
        d_tax = district_info['d_tax']
        d_next_o_id = district_info['d_next_o_id']

        prepared_query = self.prepareQuery(q.getCustomer)
        customer_info = tx.execute(
                prepared_query, {
                    '$c_w_id': w_id,
                    '$c_d_id': d_id,
                    '$c_id': c_id,
                }
        )
        customer_info = customer_info[0].rows[0]
        c_discount = customer_info['c_discount']

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        
        prepared_query = self.prepareQuery(q.incrementNextOrderId)
        result = tx.execute(
                prepared_query, {
                    '$d_id': d_id, 
                    '$d_w_id': w_id
                }
        )
        prepared_query = self.prepareQuery(q.createNewOrder)
        result = tx.execute(
                prepared_query, {
                    '$no_o_id': d_next_o_id, 
                    '$no_d_id': d_id, 
                    '$no_w_id': w_id
                }
        )

        prep_o_entry_d = prepare_datetime(o_entry_d)
        prepared_query = self.prepareQuery(q.createOrder)
        result = tx.execute(
                prepared_query, {
                    '$o_id': d_next_o_id, 
                    '$o_d_id': d_id, 
                    '$o_w_id': w_id, 
                    '$o_c_id': c_id, 
                    '$o_entry_d': prep_o_entry_d,
                    '$o_carrier_id': o_carrier_id, 
                    '$o_ol_cnt': ol_cnt, 
                    '$o_all_local': all_local
                }
        )
        
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

            #TODO use itemInfo don't split
            itemInfo = items[i]
            i_name = itemInfo['i_name']
            i_data = itemInfo['i_data']
            i_price = itemInfo['i_price']
            
            #TODO
            prepared_query = self.prepareQuery(q.getStockInfo, d_id)

            stockInfo = tx.execute(
                    prepared_query, {
                        '$s_i_id': ol_i_id,
                        '$s_w_id': ol_supply_w_id,
                    }
            )
            stockInfo = stockInfo[0].rows[0]
            
            if len(stockInfo) == 0:
                logging.warn("No STOCK record for (ol_i_id={}, ol_supply_w_id={})".format(ol_i_id, ol_supply_w_id))
                continue

            s_quantity = stockInfo['s_quantity']
            s_ytd = stockInfo['s_ytd']
            s_order_cnt = stockInfo['s_order_cnt']
            s_remote_cnt = stockInfo['s_remote_cnt']
            s_data = stockInfo['s_data']
            s_dist_xx = stockInfo['s_dist_{:02d}'.format(d_id)] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            
            if ol_supply_w_id != w_id: s_remote_cnt += 1
            #TODO
            prepared_query = self.prepareQuery(q.updateStock)
            result = tx.execute(
                    prepared_query, {
                        '$s_quantity': s_quantity, 
                        '$s_ytd': s_ytd,
                        '$s_order_cnt': s_order_cnt,
                        '$s_remote_cnt': s_remote_cnt,
                        '$s_i_id': ol_i_id, 
                        '$s_w_id': ol_supply_w_id, 
                    }
            )
            if i_data.decode().find(constants.ORIGINAL_STRING) != -1 and s_data.decode().find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount
            #TODO change type $ol_amount to Decimal(6,2): create_tables.sql & ydb_queries.py
            
            #TODO 1)use bulk insert, otherwise got error:
            # "Failed to execute Transaction 'NEW_ORDER': message: "Execution" issue_code: 1060 severity: 1 issues { position { row: 14 column: 21 file: "" } message: "Operation \'InsertAbort\' can\'t be performed on previously modified table: /ru/home/ivan-kush/mydb/order_line" end_position { row: 14 column: 21 } issue_code: 2008 severity: 1 }"
            # currently hack using UPSERT
            prepared_query = self.prepareQuery(q.createOrderLine)
            result = tx.execute(
                prepared_query, {
                    '$ol_o_id': d_next_o_id, 
                    '$ol_d_id': d_id,
                    '$ol_w_id': w_id,
                    '$ol_number': ol_number,
                    '$ol_i_id': ol_i_id,
                    '$ol_supply_w_id': ol_supply_w_id, 
                    '$ol_delivery_d': prep_o_entry_d,
                    '$ol_quantity': ol_quantity,
                    '$ol_amount': ol_amount,
                    '$ol_dist_info': s_dist_xx
                }
            )
            
            # Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
            break
        ## FOR
        # Commit!
        tx.commit()

        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        #TODO returns
        return [ customer_info, misc, item_data ]

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = queries.ORDER_STATUS
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        tx = self.session.transaction(ydb.SerializableReadWrite()).begin()

        if c_id != None:
            prepared_query = self.prepareQuery(q.getCustomerByCustomerId)
            customer = tx.execute(
                prepared_query, {
                    '$c_id': c_id,
                    '$c_d_id': d_id,
                    '$c_w_id': w_id
                }
            )
            customer = customer[0].rows[0]
        else:
            # Get the midpoint customer's id
            prepared_query = self.prepareQuery(q.getCustomersByLastName)
            all_customers = tx.execute(
                prepared_query, {
                    '$c_last': c_last,
                    '$c_d_id': d_id,
                    '$c_w_id': w_id
                }
            )
            all_customers = all_customers[0].rows[0]
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = int((namecnt - 1) / 2) #TODO float...
            customer = all_customers[index]
            c_id = customer['c_id']
        assert len(customer) > 0
        assert c_id != None
        
        prepared_query = self.prepareQuery(q.getLastOrder)
        order = tx.execute(
            prepared_query, {
                '$o_c_id': c_id,
                '$o_d_id': d_id,
                '$o_w_id': w_id
            }
        )
        order = order[0].rows[0]

        if order:
            prepared_query = self.prepareQuery(q.getOrderLines)
            orderLines = tx.execute(
                prepared_query, {
                    '$ol_o_id': order['o_id'],
                    '$ol_d_id': d_id,
                    '$ol_w_id': w_id
                }
            )
            orderLines = orderLines[0].rows[0]
        else:
            orderLines = [ ]

        tx.commit()
        #TODO returns
        return [ customer, order, orderLines ]

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        q = queries.PAYMENT

        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        tx = self.session.transaction(ydb.SerializableReadWrite()).begin()
        
        if c_id != None:
            prepared_query = self.prepareQuery(q.getCustomerByCustomerId)
            customer = tx.execute(
                prepared_query, {
                    '$c_id': c_id,
                    '$c_d_id': d_id,
                    '$c_w_id': w_id
                }
            )
            customer = customer[0].rows[0]
        else:
            # Get the midpoint customer's id
            prepared_query = self.prepareQuery(q.getCustomersByLastName)
            all_customers = tx.execute(
                prepared_query, {
                    '$c_last': c_last,
                    '$c_d_id': d_id,
                    '$c_w_id': w_id
                }
            )
            all_customers = all_customers[0].rows[0]
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = int((namecnt - 1) / 2)
            customer = all_customers[index]
            c_id = customer['c_id']
        assert len(customer) > 0
        c_balance = customer['c_balance'] - h_amount
        c_ytd_payment = customer['c_ytd_payment'] + h_amount
        c_payment_cnt = customer['c_payment_cnt'] + 1
        c_data = customer['c_data']

        prepared_query = self.prepareQuery(q.getWarehouse)
        warehouse = tx.execute(
            prepared_query, {
                '$w_id': w_id
            }
        )
        warehouse = warehouse[0].rows[0]
        
        prepared_query = self.prepareQuery(q.getDistrict)
        district = tx.execute(
            prepared_query, {
                '$d_id': d_id
                '$d_w_id': w_id
            }
        )
        district = district[0].rows[0]
        
        prepared_query = self.prepareQuery(q.updateWarehouseBalance)
        tx.execute(
            prepared_query, {
                '$w_id': w_id,
                '$delta_w_ytd': h_amount
            }
        )

        prepared_query = self.prepareQuery(q.updateDistrictBalance)
        tx.execute(
            prepared_query, {
                '$d_w_id': w_id,
                '$d_d_id': d_id,
                '$delta_d_ytd': h_amount
            }
        )

        # Customer Credit Information
        if customer['c_credit'] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]

            prepared_query = self.prepareQuery(q.updateBCCustomer)
            customer = tx.execute(
                prepared_query, {
                    '$c_balance': c_balance,
                    '$c_ytd_payment': c_ytd_payment,
                    '$c_payment_cnt': c_payment_cnt,
                    '$c_data': c_data,
                    '$c_id': c_id,
                    '$c_d_id': d_id,
                    '$c_w_id': w_id
                }
            )
            customer = customer[0].rows[0]
        else:
            c_data = ""
            prepared_query = self.prepareQuery(q.updateGCCustomer)
            customer = tx.execute(
                prepared_query, {
                    '$c_balance': c_balance,
                    '$c_ytd_payment': c_ytd_payment,
                    '$c_payment_cnt': c_payment_cnt,
                    '$c_id': c_id,
                    '$c_d_id': d_id,
                    '$c_w_id': w_id
                }
            )
            customer = customer[0].rows[0]

        # Concatenate w_name, four spaces, d_name
        h_data = '{}    {}'.format(warehouse['w_id'], district['d_id'])
        # Create the history record
        prepared_query = self.prepareQuery(q.insertHistory)
        tx.execute(
            prepared_query, {
                '$h_c_id': c_id,
                '$h_c_d_id': c_d_id,
                '$h_c_w_id': c_w_id,
                '$h_d_id': d_id,
                '$h_w_id': w_id,
                '$h_date': self.prepare_datetime(h_date),
                '$h_amount': h_amount,
                '$h_data': h_data,
            }
        )

        tx.commit()

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        #TODO returns
        return [ warehouse, district, customer ]
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        q = queries.STOCK_LEVEL

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        tx = self.session.transaction(ydb.SerializableReadWrite()).begin()
        
        #TODO
        prepared_query = self.prepareQuery(q.getOId)
        stockInfo = tx.execute(
                prepared_query, {
                    '$d_w_id': w_id, 
                    '$d_id': d_id,
                }
        )
        stockInfo = stockInfo[0].rows[0]
        assert result
        o_id = stockInfo['d_next_o_id']
        
        #TODO
        prepared_query = self.prepareQuery(q.getStockCount)
        stockCountInfo = tx.execute(
            prepared_query, {
                '$ol_w_id': w_id, 
                '$ol_d_id': d_id, 
                '$ol_o_id_max': o_id,
                '$ol_o_id_min': (o_id - 20),
                '$s_w_id': w_id,
                '$s_quantity_max': threshold
            }
        )
        stockCountInfo = stockCountInfo[0].rows[0]
        
        tx.commit()
        #TODO
        return int(stockCountInfo[0])
        
## CLASS
