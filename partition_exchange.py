import pymysql
import logging
from datetime import datetime
import os
import sys
import argparse
import configparser
import cryptography

parser = argparse.ArgumentParser(prog='mysql-partition-exchanger', add_help=False, description='MySQL utility to exchange bulk partitions ')
parser.add_argument("--host", "-h", help="The Hostname/IP ofdatabase " )
parser.add_argument("--help", action='help', help='show this help message and exit')
parser.add_argument("--user", "-u", help="The user to connect to database, if not specified it will look in ~/.my.cnf " )
parser.add_argument("--password", "-p", help="The Password to connect to database, if not specified it will look in ~/.my.cnf " )
parser.add_argument("--port", "-P", help="The Port to connect to database, if not specified it will look in ~/.my.cnf " )
parser.add_argument("--table", "-t", help="The table whose partitions should be exchanged" )
parser.add_argument("--database", "-d", help="The database or schema name" )
parser.add_argument("--condition", "-c", help="The condition that must be applied to check for partitions to be dropped. It will be checked in NFORMATION_SCHEMA.PARTITIONS " )
parser.add_argument("--dry-run", "-dr", help="Specifying this will not run modify or create any table. Only checks will be done and queries will be printed " )
parser.add_argument("--log-file", "-l", help="log file name " )
parser.add_argument("--lock-wait-timout", "-lt", help="Lock timeout. If not provided default value of 5 seconds is applied" )
parser.add_argument("--exit-if-fail", "-e", action='store_true', help="If enabled it will continue to exchange other partitions if it fails for one ")
parser.add_argument("--retries", "-r", help="no of retries before it should exit" )
parser.add_argument("--exchange_name", "-en", help="Suffix Name of intermediary table that should act as template for other tables. If not provide, default name source_table_template will be used" )

args = parser.parse_args()
args_dict={}
for k,v in dict(vars(args)).items():
  args_dict[k]=v
  
  


if(args_dict['database'] is None):
  print("No Database provided, please check.")
  sys.exit(0)
else:
  schema=args_dict['database']


if(args_dict['table'] is None):
  print("No Table provided, please check.")
  sys.exit(0)
else:
  table=args_dict['table']

if(args_dict['exchange_name'] is None):
  exchange_name="template"
else:
  exchange_name=args_dict['exchange_name']
if(args_dict['condition'] is None):
  print("No Partition check condition provided, please check.")
  sys.exit(0)
else:
  cndtn=args_dict['condition']
if(args_dict['dry_run'] is None):
  
  dry_run=False
else:
  
  dry_run=True


if(args_dict['log_file'] is None):
  logger=logging.getLogger()
  logger.disabled = True
else:
  logging.basicConfig(filename=args_dict['log_file'],
                    format='%(asctime)s %(message)s',
                    filemode='w')
  logger=logging.getLogger()  

if(args_dict['lock_wait_timout'] is None):
  lock_wait_tiemout=5
else:
  lock_wait_tiemout=args_dict['lock_wait_timout']
  
if(args_dict['exit_if_fail'] is None):
  exit_status=True
else:
  exit_status=False

if(args_dict['retries'] is None):
  max_retries=1
else:
  max_retries=int(args_dict['retries'])

now = datetime.now()
date_time = now.strftime("%Y_%m_%d_%H_%M_%S")







logger.setLevel(logging.INFO)


def main():
  
  host=args_dict['host']
  user=args_dict['user']
  password=args_dict['password']
  port=args_dict['port']

  cursor=db_cursor(host,user,password,schema,port)
  check_table_part(cursor,schema,table)

  part_names=part_cndtns(cursor,schema,table,cndtn)
  if(len(part_names)>0):

    
    create_first_table(cursor,schema,table,exchange_name)
    check_names=set()
    for item in part_names:
      check_names.add("_"+table+"_"+item[0])


    diff=check_table_exist(cursor,schema,check_names);
    if(len(diff)!=len(check_names)):
      print("These table exists, please delete these "+str(check_names-diff))
      print("Exiting")
      sys.exit(0)
    else :
      create_tables(cursor,table,exchange_name,part_names)
      
      for i in range(0,len(part_names)):
        status = exchange_partition(cursor,table,part_names[i][0])
        if status==0 and exit_status is True:
          logger.info("Not continuting as exit-if-fail is true")
          sys.exit(0)

  else :
    print("No Partitions to exchange")
    print("Exiting..")

def check_table_part(cursor,schema,table):
  
  check_query="SELECT count(*) from INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA='"+schema+"' AND TABLE_NAME='"+table+"'"
  logger.info("Checking table for partitions")
  cursor.execute(check_query)

  count=cursor.fetchall()
  
  if(count[0][0]<2):
    print("This table has no partitions")
    sys.exit(0)
  logger.info("Partitions Found")

def create_tables(cursor,source_table,exchange_name,part_names):
  for i in range(0,len(part_names)):
    create_query="CREATE TABLE _"+source_table+"_"+part_names[i][0]+" LIKE _"+source_table+"_"+exchange_name+";"
    logger.info("Creating  _"+source_table+"_"+part_names[i][0]+" Table")
    if(dry_run is False):
      cursor.execute(create_query)
    else:
      print(create_query)

def part_cndtns(cursor,schema,table,cndtn):
  partition_query="SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA='"+schema+"' AND TABLE_NAME='"+table+"'  AND PARTITION_NAME IS NOT NULL AND "+cndtn
  logger.info("Fetching partitions based on the query: "+partition_query)
  cursor.execute(partition_query)
  part_names=cursor.fetchall()
  logger.info("Partitions fetched")
  return part_names

def db_cursor(host,user,password,schema,port):
  try:
    logger.info("Connecting")
    
    mysql_config=read_config(host,user,password,schema,port)
    connection = pymysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        passwd=mysql_config['password'],
        db=mysql_config['database'],
        port=mysql_config['port'])
    cursor = connection.cursor()
    logger.info("Connected")
    

    cursor.execute("SET lock_wait_timeout="+str(lock_wait_tiemout))
    return cursor
  except Exception as e: 
    logger.info(str(e))
    print(e)
    sys.exit(0)
    
def read_config(host,user,password,schema,port):
  config = configparser.ConfigParser()
  logger.info("Checking ~/.my.cnf")  

  config.read(os.path.expanduser('~/.my.cnf'))
  

  mysql_config = {
        'user': user,
        'password': password,
        'host': host,
        'database': schema,
        'port':port
  }
    
    # Update parameters from .my.cnf if not provided
  if 'client' in config:
    if user is None and config['client'].get('user'):
      mysql_config['user'] = config['client']['user']
    elif user is None and config['client'].get('user') is None:
      print("No user provided")
      sys.exit(0)
    
      
    if password is None and config['client'].get('password'):
      mysql_config['password'] = config['client']['password']
    elif password is None and config['client'].get('password') is None:
      print("No password provided")
      sys.exit(0)
    
    if host is None and config['client'].get('host'):
      mysql_config['host'] = config['client']['host']
    elif host is None and config['client'].get('host') is None:
      mysql_config['host']='127.0.0.1'
    
    if schema is None and config['client'].get('database'):
      mysql_config['database'] = config['client']['database']
    elif schema is None and config['client'].get('database') is None:
      print("No database provided")
      sys.exit(0)

    if port is None and config['client'].get('port'):
      mysql_config['port'] = config['client']['port']
    elif port is None and config['client'].get('port') is None:
      mysql_config['port']=3306
    return mysql_config
    

def create_first_table(cursor,schema,source_table,exchange_name):
  first_table=set()
  first_table.add("_"+source_table+"_"+exchange_name)
  logger.info("Creating First table")
  
  result=check_table_exist(cursor,schema,first_table)
  
  if(len(result)!=len(first_table)):
    table_msg="_"+source_table+"_"+exchange_name+ " already exists."
    exit_msg="Exiting"
    logger.info(table_msg)
    logger.info(exit_msg)
    print(table_msg)
    print(exit_msg)
    sys.exit(0)
  else:
    create_query="CREATE TABLE _"+source_table+"_"+exchange_name+" LIKE "+source_table+";"
    remove_part_query="ALTER TABLE _"+source_table+"_"+exchange_name+" REMOVE PARTITIONING;"
   
    
    if(dry_run is False):
      cursor.execute(create_query)
    else :
      print(create_query)
    logger.info("Table Created")
    logger.info(remove_part_query)
    if(dry_run is False):
      cursor.execute(remove_part_query)
    else :
      print(remove_part_query)
    logger.info("Partitioning removed from template table")

def check_table_exist(cursor,schema,source_table):
  str_table = ""
  
  src_table=set()
  for item in source_table:
    logger.info(item)
    str_table =str_table+"'"+item+"',"
    src_table.add(item)
  logger.info("checking if table exists" +str_table)
  str_table=str_table[:-1]
  check_query="SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='"+schema+"' AND TABLE_NAME IN("+str_table+") ORDER BY 1"
  
  cursor.execute(check_query)
  my_set=set()
  table_names=cursor.fetchall()
  

  
  for i in table_names:
    my_set.add(i[0])
  
  
  diff= source_table - my_set
  
  diff_str=""
  if(len(diff)==0):
    diff_str=str(source_table)
  else:
    diff_str=str(diff)

  logger.info("Tables not in database :"+str(diff))

  return diff
  
  
  
  



def exchange_partition(cursor,source_table,partition_name):
  
  exchange_query="ALTER TABLE "+source_table+" EXCHANGE PARTITION "+partition_name+" WITH TABLE _"+source_table+"_"+partition_name+";"
  logger.info("Exchanging Partitions")
  logger.info(exchange_query)
  tries=0
  
  status=0
  if(dry_run is False):
    while tries<max_retries :
      try:
        cursor.execute(exchange_query)
        logger.info("Exchanged")
        status=1
        break
      except pymysql.Error as e: 
        tries += 1
        logger.info("The attempt failed for " + partition_name+". Attempt no."+str(tries))
        print("Failed Attempt  :"+str(tries))
        print("Failed for :"+ partition_name)
        print(e)
        status=0
    return status
  else:
    
    print(exchange_query)
    return 1

if __name__ =="__main__":
  main()
