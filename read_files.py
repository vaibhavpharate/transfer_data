from datetime import datetime
import pandas as pd
import numpy as np
import netCDF4 as nc
import logging
import os

from configs.paths import destination_path, destination_ip
from configs.db_conf import *
from database_funcs import get_connection

 
# Create and configure logger
logging.basicConfig(filename="logs/read_files.log",
                    format='%(asctime)s %(levelname)s %(message)s',
                    filemode='a+')
 
# Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.INFO)



read_files_log = 'logs/read_file.csv'
transfer_files_log = 'logs/transfer.csv'


if os.path.exists(read_files_log)==False:
    df = pd.DataFrame({'timestamp':[],'variable':[],'status':[],'log_ts':[]})
    df.to_csv(read_files_log)
else:
    df = pd.read_csv(read_files_log)

# read_variables = ['CMA','CT','CMIC','CRR','CRRPh','CTTH']
read_variables = ['CT']
folder_format = "%Y%m%d"
file_timestamp = folder_format+"T%H%M00Z"



def read_transfer_files_logs(tf_log_path:str):
    if os.path.exists(tf_log_path):
        transfer_logs = pd.read_csv(tf_log_path)
        transfer_logs['timestamp'] = pd.to_datetime(transfer_logs['timestamp'],format="%Y-%m-%d %H:%M:%S")
        return transfer_logs
    else:
        logger.error("There is no transfer logs file please execute transfer files script")
    

def check_if_data_exists(timestamp,db_connection):
    df = pd.read_sql_query(f"SELECT COUNT(*) FROM haleware.satellite_data WHERE timestamp = '{timestamp}'"
                           ,db_connection)
    print(df['count'])
    if df['count'][0] > 0:
        return False
    else:
        return True
    # return df


## This will read the infividual files
def data_to_database(timestamp,file_path,db_connection,trf_df,variable_atts):
    # if check_if_data_exists(timestamp=timestamp,db_connection = db_connection):
        timestamp = str(timestamp)

        if os.path.exists(file_path):

            data = nc.Dataset(file_path)
            df = pd.DataFrame()

            ## Setting up the timestamp, lat and lon
            
            df['lat'] = np.array(data.variables['lat'][:]).flatten()
            df['lon'] = np.array(data.variables['lon'][:]).flatten()
            df['timestamp'] = timestamp
            # df['timestamp'] = pd.to_datetime(df['timestamp'])
            for i in variable_atts['CT']:
                df[i] = np.array(data.variables[i][:]).flatten()
            try:
                df.to_sql(name='satellite_data',schema='haleware',if_exists='append',index=False,con=db_connection)
                trf_df.loc[trf_df['timestamp']==timestamp,'read_status'] = 1 
                logger.info(f"Transfered Data for timestamp {timestamp}")
                # trf_df.drop(['file_path','date'],axis=1,inplace=True)
                
            except Exception as e:
                logger.warning(f"Error Occured {e}")
        else:
            print("File Does not exists")
        
        trf_df.to_csv(transfer_files_log,index=False)






# data_to_database(timestamp=tracker_df['timestamp'].head(3)[1],
#                  file_path=tracker_df['file_path'].head(3).values[1],
#                  db_connection=db_connection)



