import csv
import pyodbc
#import getpass
import logging
import datetime
import config

logging.basicConfig(format='%(asctime)s %(message)s', filename='log/teradata.log', level=logging.DEBUG)
log = logging.getLogger("my-logger")
logging.basicConfig(format='%(asctime)s %(message)s', filename='log/teradata_summary.log', level=logging.DEBUG)
log_summary = logging.getLogger("log-summary")
             
#username = getpass.getuser()
#password = getpass.getpass(prompt='Please enter your teradata password:')
#add AUTHENTICATION=LDAP; to config if you are using ldap credentials

#please create config file if it doesn't exist
username = config.teradata_username
password = config.teradata_password

print(username)

pyodbc.pooling = False
cnx = pyodbc.connect("DRIVER={Teradata};DBCNAME=dwprod1.corp.linkedin.com;DATABASE=DWH;UID=%s;PWD=%s" % (username, password), autocommit=True, ANSI=True)
cursor = cnx.cursor()

#create date_string
today = datetime.date.today()
print(today)
mydate = str(today.year) + '_' + str(today.month) + '_' + str(today.day)

with open("scraped_data/google_jobs_results_" + mydate + ".tsv") as input:
    mydata = [line.strip().split('\t') for line in input]
    
current_progress = 0
error_count = 0
for i in range(1, len(mydata)):
    if len(mydata[i]) == 13:
        mydatafinal = mydata[i][0:10] + [mydata[i][10][0:31000]] + [mydata[i][10][31001:62000]] + [mydata[i][10][62001:93000]] + mydata[i][11:12]
        for j in range(len(mydatafinal)):
            mydatafinal[j] = mydatafinal[j].replace("'","''")     
        try:
            sql = """INSERT INTO dm_biz.google_jobs_crawl5 
            VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', current_time)""".format(*mydatafinal)
        except SyntaxError as e:
            print("Failed to generate sql")
            log.error("Failed to generate sql at index: " + str(i))
            log.error("Failed to generate sql at index: " + str(e))
            error_count += 1
        try:
            cursor.execute(sql)
        except Exception as e:
            print("failed to insert data")
            log.error("failed to generate sql at index" + str(i))
            log.error(str(mydatafinal))
            log.error('Failed to do something: ' + str(e))
            error_count += 1
        print("current_progress: " + str(current_progress))
        current_progress += 1

log_summary.info("total number of errors: " + str(error_count))        

cursor.close()
del cursor
cnx.close()