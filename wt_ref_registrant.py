
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
    SELECT CAST(a.record_id as nvarchar(30)) as 'registrant_id'
        , rtrim(CAST(a.email as nvarchar(100))) as 'email'
        , CAST(a.orcid as nvarchar(80)) as 'orcid'
    FROM DW_Staging.tango_client.accounts a WITH (NOLOCK)
    WHERE a.last_modified >= CONVERT(VARCHAR(8), DATEADD(month, DATEDIFF(month, 0, GETDATE())-1, 0), 112)
    """

## mongo variables
m_database = 'wt_ref'
m_host = '127.0.0.1'       
m_port = 27017
m_collection = 'registrant'

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
            email = rec[1].encode('utf-8')
            record = {"email":email}
        
            if rec[2] != None:
                orcid = rec[2].encode('utf-8')   
                record.update({"orcid":orcid})
        
            #m_col.insert_one(record)
            m_col.update_one({"_id":int(rec[0])},{"$set":record} , upsert=True)
           
        except Exception as e:
            #print e
            pass
        
        if s_count % 10000 == 0:
            print str(datetime.now()) + " record count " + str(s_count)
            
    ## finish up
    finish_up(m_con, s_con)
