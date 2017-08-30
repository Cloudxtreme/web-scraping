import csv
import pyodbc
import logging
from logging import FileHandler
import datetime
import config
import sys

def retry_request(failed_sql_list):
    """
    inputs:
        failed_sql_list: list of sql queries to be executed
    output:
        new_failed_sql_list: new list of sql queries to be executed in the next while loop (see main() to see how this is used.)

    Takes as input a list of sql queries and tries to execute each one.  
    If that fails, then the sql query is broken down into two pieces and retried again.
    """

    new_failed_sql_list = []
    for i, sql in enumerate(failed_sql_list):
        try:
            cursor.execute(sql)
            print("insert of partition " + str(i) + " success!")
        except Exception as e:
            print("failed to insert partition " + str(i) + ", retrying with even smaller batch size...")
            log.error("failed to insert partition 1" + str(i))
            log.error(str(e))

            sql_groups = sql.split(';')
            n = len(sql_groups)/2

            total_sql_1 = ';'.join(sql_groups[:n])
            total_sql_2 = ';'.join(sql_groups[n:])
            new_failed_sql_list.append(total_sql_1)
            new_failed_sql_list.append(total_sql_2)
            
    return new_failed_sql_list

if __name__ == "__main__":
    SUMMARY_LOG_FILE = "log/teradata_summary.log"
    logging.basicConfig(format='%(asctime)s %(message)s', filename='log/teradata.log', level=logging.DEBUG)
    log = logging.getLogger("my-logger")

    log_summary = logging.getLogger("log-summary")
    log_summary_file_handler = FileHandler(SUMMARY_LOG_FILE)
    log_summary.addHandler(log_summary_file_handler)
                 
    #please create config file if it doesn't exist
    username = config.teradata_username
    password = config.teradata_password

    print(username)

    #create date_string
    today = datetime.date.today()
    mydate0 = str(today.year) + '_' + str(today.month) + '_' + str(today.day)
    
    try:
        mydate = sys.argv[1] 
    except:
        mydate = mydate0
    
    print(mydate)
    with open("scraped_data/google_jobs_results_" + mydate + ".tsv") as input:
        mydata = [line.strip().split('\t') for line in input]

    pyodbc.pooling = False
    #add AUTHENTICATION=LDAP; to config if you are using ldap credentials
    cnx = pyodbc.connect("DRIVER={Teradata};DBCNAME=dwprod1.corp.linkedin.com;DATABASE=DWH;UID=%s;PWD=%s" % (username, password), autocommit=True, ANSI=True)
    cursor = cnx.cursor()

    #set the number of insert statements per execution
    batch_size       = 100
    current_progress = 0
    error_count      = 0
    total_sql        = ""

    for i in range(1, len(mydata)):
        if len(mydata[i]) == 13:
            mydatafinal = mydata[i][0:10] + [mydata[i][10][0:31000]] + [mydata[i][10][31001:62000]] + [mydata[i][10][62001:93000]] + mydata[i][11:13]
            for j in range(len(mydatafinal)):
                mydatafinal[j] = mydatafinal[j].replace("'","''")     
            try:
                sql = """INSERT INTO dm_biz.google_jobs_crawl5
                VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', CAST('{14}' AS TIMESTAMP(0) FORMAT 'YYYYMMDDHHMISS'));""".format(*mydatafinal)
            except SyntaxError as e:
                print("Failed to generate sql")
                log.error("Failed to generate sql at index: " + str(i))
                log.error("Failed to generate sql with error: " + str(e))

            total_sql += sql
            if i % batch_size == 0:
                try:
                    cursor.execute(total_sql)
                except Exception as e:
                    print("failed to insert data, retrying with smaller batch size...")
                    log.error("failed to generate sql at index" + str(i))
                    log.error(str(mydatafinal))
                    log.error(str(e))
                    error_count += 1

                    #try again, but spltting into two separate requests.
                    n = batch_size // 2
                    sql_groups = total_sql.split(';')

                    total_sql_1 = ';'.join(sql_groups[:n])
                    total_sql_2 = ';'.join(sql_groups[n:])

                    failed_sql_list = []
                    failed_sql_list.append(total_sql_1)
                    failed_sql_list.append(total_sql_2)

                    while len(failed_sql_list) != 0:
                        failed_sql_list = retry_request(failed_sql_list)
                finally:
                    current_progress += batch_size
                    print("current_progress: " + str(current_progress))
                    total_sql = ""

    log_summary.info(str(mydate) + ": total number of errors: " + str(error_count))        
    cursor.close()
    del cursor
    cnx.close()













