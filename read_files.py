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
logger.setLevel(logging.DEBUG)



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


variable_atts = {'CTTH':['ctth_pres', 'ctth_alti', 'ctth_tempe', 'ctth_effectiv', 'ctth_method', 'ctth_status_flag'],
 'CMA':['cma_cloudsnow', 'cma', 'cma_dust', 'cma_volcanic', 'cma_smoke', 'cma_testlist1', 'cma_testlist2', 'cma_status_flag', 'cma_conditions'],
 'CRRPh':['crrph_intensity', 'crrph_accum', 'crrph_status_flag', 'crrph_conditions'],
 'CMIC':['cmic_phase', 'cmic_reff', 'cmic_cot', 'cmic_lwp', 'cmic_iwp', 'cmic_status_flag', 'cmic_conditions'],
 'CRR':['crr', 'crr_intensity', 'crr_accum', 'crr_status_flag', 'crr_conditions'],
 'CT':['ct']
}

def read_transfer_files_logs(tf_log_path:str):
    if os.path.exists(tf_log_path):
        transfer_logs = pd.read_csv(tf_log_path)
        transfer_logs['timestamp'] = pd.to_datetime(transfer_logs['timestamp'],format="%Y-%m-%d %H:%M:%S")
        return transfer_logs
    else:
        logger.error("There is no transfer logs file please execute transfer files script")
    



## Check for the data folders
trf_df = read_transfer_files_logs(transfer_files_log)
tracker_df = trf_df.copy()
tracker_df['date'] = pd.to_datetime(tracker_df['timestamp'].dt.date)
tracker_df['file_path'] = destination_path +"/"+tracker_df['date'].dt.strftime(folder_format) + "/" + tracker_df['file']
# print(trf_df)


db_connection = get_connection(host = data_configs_local['host'],
                              port = data_configs_local['port'],
                              user = data_configs_local['user'],
                              passord= data_configs_local['password'],
                              database= data_configs_local['database'])

def check_if_data_exists(timestamp):
    df = pd.read_sql_query(f"SELECT COUNT(*) FROM haleware.satellite_data WHERE timestamp = '{timestamp}'"
                           ,db_connection)
    print(df['count'])
    if df['count'][0] > 0:
        return False
    else:
        return True
    # return df


## This will read the infividual files
def data_to_database(timestamp,file_path,db_connection):
    # if check_if_data_exists(timestamp=timestamp):
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


transfer_df = tracker_df.loc[trf_df['read_status']==0,:]
for index, row in transfer_df.iterrows():
    data_to_database(timestamp=row['timestamp'],
                 file_path=row['file_path'],
                 db_connection=db_connection)




# data_to_database(timestamp=tracker_df['timestamp'].head(3)[1],
#                  file_path=tracker_df['file_path'].head(3).values[1],
#                  db_connection=db_connection)



