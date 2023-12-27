from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import netCDF4 as nc
import logging
import os

from sqlalchemy import text

from configs.paths import destination_path, destination_ip,directory_path
from configs.db_conf import *
from database_funcs import get_connection, get_ci_ct_map

 
# Create and configure logger
logging.basicConfig(filename=f"{directory_path}/logs/read_files.log",
                    format='%(asctime)s %(levelname)s %(message)s',
                    filemode='a+')
 
# Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.INFO)
read_files_log = f'{directory_path}/logs/read_file.csv'
transfer_files_log = f'{directory_path}/logs/transfer.csv'
# 

#read_files_log = 'logs/read_file.csv'
#transfer_files_log = 'logs/transfer.csv'


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
    if True:
        timestamp = str(timestamp)
        previous_48 = datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S') - timedelta(hours=48)
        previous_48 = previous_48.strftime(format='%Y-%m-%d %H:%M:%S')
        ct_data={1: 'Cloud-free_land',
            5: 'Very_low_clouds',
            6: 'Low_clouds',
            7: 'Mid-level_clouds',
            8: 'High_opaque_clouds',
            9: 'Very_high_opaque_clouds',
            10: 'Fractional_clouds',
            11: 'High_semitransparent_thin_clouds',
            12: 'High_semitransparent_moderately_thick_clouds',
            13: 'High_semitransparent_thick_clouds',
            14: 'High_semitransparent_above_low_or_medium_clouds',
            2: 'Cloud-free_sea',
            3: 'Snow_over_land',
            4: 'Sea_ice'}
        
        home_conn = get_connection(host = conn_dict['host'],
                              port = conn_dict['port'],
                              user = conn_dict['user'],
                              passord= conn_dict['password'],
                              database= conn_dict['database'])

        try:
            with db_connection.connect() as conn:
                conn.execute(text(f"DELETE FROM haleware.satellite_data  WHERE timestamp <='{previous_48}'"))
                print(f"Deleted Data Before and including timestamp {previous_48}")
        except Exception as e:
            print(e)
        if os.path.exists(file_path):

            data = nc.Dataset(file_path)
            df = pd.DataFrame()
            ## Setting up the timestamp, lat and l	            
            df['lat'] = np.array(data.variables['lat'][:]).flatten()
            df['lon'] = np.array(data.variables['lon'][:]).flatten()
            df['timestamp'] = timestamp

            # db_connection_2 = get_connection()

            ci_ct_map = get_ci_ct_map(db_connection=home_conn)
            ## Delete the previous_timestamps of 48 hours before

            for i in variable_atts['CT']:
                df[i] = np.array(data.variables[i][:]).flatten()
            try:
                df['ct_flag'] = df['ct'].apply(lambda x: ct_data[x])
                df['ci_data'] = df['ct_flag'].apply(lambda x:ci_ct_map[x])
                # print(df)

                if check_if_data_exists(timestamp=timestamp,db_connection=db_connection):
                    df.to_sql(name='satellite_data',schema='haleware',if_exists='append',index=False,con=db_connection)
                    trf_df.loc[trf_df['timestamp']==timestamp,'read_status'] = 1 
                    logger.info(f"Transfered Data for timestamp {timestamp}")
                else:
                    logger.warning(f"Data for timestamp {timestamp} already exists")
                
                
            except Exception as e:
                logger.warning(f"Error Occured {e}")
                print(e)
        else:
            print("File Does not exists")
        
        trf_df.to_csv(transfer_files_log,index=False)



