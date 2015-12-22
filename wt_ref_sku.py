
## imports
import pyodbc
from datetime import datetime
from pymongo import MongoClient

## variables
s_server = 'MPL-LDN-NPGDB2'
s_database = 'DW_Staging'
s_user = 'readonly_user'
s_password = 'Warehouse'
s_query = """
        WITH cte_sub AS
        (
            SELECT sd.subscription_def_id, sd.description, sd.allow_on_internet
		, od.perpetual_order, od.no_charge, od.active
		, res.product_code, res.product_desc		
		, CASE WHEN sd.subscription_category_id = 31 THEN CAST(sd.subscription_def_id as varchar) ELSE CAST(sd.subscription_def_id as varchar) + '_SUB'END AS 'id'
		, CASE WHEN sd.subscription_category_id = 31 THEN 'article' ELSE 'subscription' END AS 'type'
		, CASE WHEN t.term = '1U7D' THEN '7D' ELSE RIGHT(t.term, 2) END AS 'term_desc'
            FROM [ECLIPSE REPORTING].eclipse_reporting.dbo.subscription_def sd
		inner join [ECLIPSE REPORTING].eclipse_reporting.dbo.term t on sd.term_id = t.term_id
		inner join [ECLIPSE REPORTING].eclipse_reporting.dbo.oc on sd.oc_id = oc.oc_id
		inner join [ECLIPSE REPORTING].eclipse_reporting.dbo.order_code od on sd.order_code_id = od.order_code_id
		inner join 
		(
			SELECT p.product_id, LOWER(p.product_code) as 'product_code', product_desc
				, oc_r.OcEclipseID
			FROm DW_Staging.tango_client.products p 	
				inner join [RESOLUTION_LIVE].Resolution.dbo.Product p_r on p.product_code collate DATABASE_DEFAULT = p_r.ProductName
				inner join [RESOLUTION_LIVE].Resolution.dbo.Oc oc_r on p_r.productid = oc_r.ProductID				
			WHERE p.product_id not in (290,1398) --handles jidsp & bonekey reports			
		) res on oc.oc_id = res.OcEclipseId
        )

        SELECT s.id
            , s.type, s.description, s.active
            , s.product_code, s.product_desc	
            , s.term_desc as 'term'
        FROM cte_sub s
        WHERE s.allow_on_internet <> 0
            and s.perpetual_order <> 1
            and s.no_charge <> 1
            and s.active = 1	

        UNION 

        SELECT distinct CAST(pd.pkg_def_id as varchar) +'_PAC' as 'id'
            , 'package' as 'type', pd.description, pd.active
            , s.product_code, s.product_desc 
            , (CAST(pd.n_calendar_units as varchar) + CASE WHEN pd.calendar_unit = 2 THEN 'M'WHEN pd.calendar_unit = 3 THEN 'Y' ELSE '?' END) AS 'term'
        FROM [ECLIPSE REPORTING].eclipse_reporting.dbo.pkg_def pd
            inner join [ECLIPSE REPORTING].eclipse_reporting.dbo.order_code od on pd.order_code_id = od.order_code_id
            inner join [ECLIPSE REPORTING].eclipse_reporting.dbo.pkg_def_content pdc on pd.pkg_def_id = pdc.pkg_def_id
            inner join cte_sub s on pdc.subscription_def_id = s.subscription_def_id	
        WHERE od.perpetual_order <> 1 
            and od.no_charge <> 1
            and pd.active = 1
            and pdc.pkg_content_seq = 1
        """

## mongo variables
m_database = 'wt_ref'
m_host = '127.0.0.1'       
m_port = 27017
m_collection = 'sku'

## functions
def connect_to_mongo (host, port, database, collection):
    m_con = MongoClient(host, port)
    m_db = m_con[database]
    m_col = m_db[collection]
    print str(datetime.now()) + " connected to " + database + "." + collection    
    return m_con, m_col

def connect_to_sql (user, password, server, database, query):
    connect_string = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' +  user + ';PWD=' + password
    s_con = pyodbc.connect(connect_string)
    s_con.autocommit = True
    s_cur = s_con.cursor()
    s_cur.execute(query)
    print str(datetime.now()) + " connected to " + s_server + "." + s_database  
    return s_con, s_cur

def finish_up (mongo_con, sql_con):
    mongo_con.close()
    sql_con.close()
    print str(datetime.now()) +  " complete"

## main
if __name__ == '__main__':

    # connect to mongo
    m_con, m_col =  connect_to_mongo(m_host, m_port, m_database, m_collection)

    # connect to sql
    s_con, s_cur = connect_to_sql(s_user, s_password, s_server, s_database, s_query)

    # loop
    s_count = 0
    for rec in s_cur:
        s_count += 1
        try:                 
            record = {
                    "type" : rec[1]
                    , "description" : rec[2]
                    , "active" : rec[3]
                    , "product_code" : rec[4]
                    , "product" : rec[5]
                    , "term" : rec[6]
                }
        
            #m_col.insert_one(record)
            m_col.update_one({"_id":rec[0]},{"$set":record} , upsert=True)
           
        except Exception as e:
            print e
            pass
        
        if s_count % 10 == 0:
            print str(datetime.now()) + " record count " + str(s_count)
            
    ## finish up
    finish_up(m_con, s_con)
