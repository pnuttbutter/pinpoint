
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
    SELECT CAST(sa.site_id as varchar(20)) as 'site_id'
        , rtrim(CAST(sa.company as nvarchar(100))) as 'company'
    FROM DW_Staging.tango_client.site_accounts sa WITH (NOLOCK)
    """

## mongo variables
m_database = 'wt_ref'
m_host = '127.0.0.1'       
m_port = 27017
m_collection = 'site'

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
    for rec in s_cur:
        try:
            company = rec[1].encode('utf-8')
            m_col.insert_one({"_id":rec[0], "company":company})
        except Exception as e:
            pass

    ## finish up
    finish_up(m_con, s_con)
