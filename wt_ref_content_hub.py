## content hub monthly
## populates the content hub collection with articles from the previous month

## imports
from datetime import datetime
from pymongo import MongoClient
from bson import json_util
import urllib2
import json

## mongo variabes
host = '127.0.0.1'        #local
port = 27017
database = 'wt_ref'
collection = 'content_hub'

## content hub variables
url = 'http://hub.nature.com/api/v1/articles'
domain = 'all'
client = 'counter'
#pub_date = '2015-11'
page_size = '20'
page_start = '1'

current_year = datetime.now().year
current_month = datetime.now().month

#year = #test year
#month = #test month

## functions
def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

def next_date (year, month):    
    if month == 12:
        year += 1
        month = 1        
    else:
        month += 1
    return year, month

def previous_date (year, month):
    if month == 1:
        year += 11
        month = 12
    else:
        month += -1
    return year, month   

def connect_to_mongo (host, port, database, collection):
    m_con = MongoClient(host, port)
    m_db = m_con[database]
    m_col = m_db[collection]
    print str(datetime.now()) + " connected to " + database + "." + collection    
    return m_con, m_col

def query_mongo (collection, query):
    m_cur = collection.find(query)
    print str(datetime.now()) + " query complete"    
    return m_cur

def build_query (url, domain, client, **kwargs):
    url_get = url + '?domain=' + domain + '&client=' + client
    for key, value in kwargs.iteritems():
        url_get = url_get + '&' + key + '=' +value
    return url_get

def query_content_hub (url):
    response = urllib2.urlopen(url)
    json_data = json.load(response)    
    articles = json_data["articles"]
    header = json_data["query"]["pagination"]
    return articles, header

def article_build (article):
    if "license" in article :
        oa_license = 1
    else:
        oa_license = 0
    article_data = {
        "_id" : article["doi"]
        , "article_id":article["id"]                                      
        , "product":article["hasJournal"]["title"]
        , "product_code":article["hasJournal"]["id"]
        , "oa_license": oa_license
    }                
    if "publicationDate" in article:
        pub_date = article["publicationDate"]                
        pd_date = datetime(int(pub_date[0:4]),int(pub_date[5:7]), int(pub_date[8:]))
        article_data.update({"pd_date": pd_date})
    if "publicationYear" in article:
        article_data.update({"yop":article["publicationYear"]})    
    if "issue" in article:
        article_data.update({"issue":article["issue"]})
    if "volume" in article:
        article_data.update({"volume": article["volume"]})
    if "title" in article:
        article_data.update({"title":article["title"].encode('utf-8')})
    if "hasPrimaryArticleType" in article:
        article_data.update({"pat":article["hasPrimaryArticleType"]["id"]})
        # front/back list logic
        if article["hasPrimaryArticleType"]["id"] in ('research', 'reviews', 'protocols', 'amendments-and-corrections'):
            article_data.update({"bh":1, "fh":0})
        else:
            article_data.update({"bh":0, "fh":1})

    return article_data

def finish_up (connection):
    connection.close()
    print str(datetime.now()) +  " complete"

## main
if __name__ == '__main__':

    ## connect to collection
    m_con, m_col =  connect_to_mongo(host, port, database, collection)    

    ## sets date variables
    end_year, end_month = next_date(current_year, current_month)
    year, month = previous_date(current_year, current_month)    # sets the year-month to the previous month
       
    ## query content hub
    #pub_year = '2015-11'

    while year < end_year or (year == end_year and month < end_month):

        pub_date = str(year) + '-' + '%02d' % month
        print str(datetime.now()) + " year month to process " + pub_date
        articles, header = query_content_hub(build_query(url, domain, client, publicationYearMonth = pub_date, page = page_start, pageSize = page_size))    
   
        ## process content hub data
        header_page = int(header["page"])
        modified = 0
        while header_page <= int(header["totalPages"]):
            print str(datetime.now()) + " processing page " + str(header_page) + " of " + str(header["totalPages"])
            for article in articles:
            
                try:
                    article_data = article_build(article)                
                    result = m_col.insert(article_data)                
                    #modified += result.modified_count 
                except Exception as e:
                    #print "ERROR - insert ", (e), article["doi"]
                    pass
            if "next" in header:
                print header["next"]
                articles, header = query_content_hub(header["next"])
                header_page = int(header["page"])
            else:
                header_page += 1

        year, month = next_date(year, month)
        
    print str(datetime.now()) + " modified count is " #+ str(modified)

    ## finish up
    finish_up(m_con)
