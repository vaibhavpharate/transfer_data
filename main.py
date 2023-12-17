from transfer_files import *
from read_files import *
import logging

# Create and configure logger
logging.basicConfig(filename="logs/files.log",
                    format='%(asctime)s %(levelname)s %(message)s',
                    filemode='a+')


# Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.INFO)
# user defined modules

folder_format = "%Y%m%d"
file_timestamp = folder_format+"T%H%M00Z"


# Get the logging csv
logger_path = 'logs/transfer.csv'
if os.path.exists(logger_path):
    df = pd.read_csv(logger_path)
else:
    df = pd.DataFrame({'timestamp':[],'variable':[],'status':[],'log_ts':[],'file':[],'read_status':[]})
    df.to_csv(logger_path,index=False)




read_variables = ['CT',]

variable_atts = {'CTTH':['ctth_pres', 'ctth_alti', 'ctth_tempe', 'ctth_effectiv', 'ctth_method', 'ctth_status_flag'],
 'CMA':['cma_cloudsnow', 'cma', 'cma_dust', 'cma_volcanic', 'cma_smoke', 'cma_testlist1', 'cma_testlist2', 'cma_status_flag', 'cma_conditions'],
 'CRRPh':['crrph_intensity', 'crrph_accum', 'crrph_status_flag', 'crrph_conditions'],
 'CMIC':['cmic_phase', 'cmic_reff', 'cmic_cot', 'cmic_lwp', 'cmic_iwp', 'cmic_status_flag', 'cmic_conditions'],
 'CRR':['crr', 'crr_intensity', 'crr_accum', 'crr_status_flag', 'crr_conditions'],
 'CT':['ct']
}


ssh_client = get_ssh()
latest_date = choose_latest_date(ssh_client=ssh_client,
                                 folder_format=folder_format,
                                 source_path=source_path,
                                 logger=logger)
# get all the variable files list in the latest date
stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}/{latest_date}')
variable_folders = stdout.readlines()
variable_folders = [str(x)[:-1] for x in variable_folders]
variable_files = seperate_files(variable_folders)



transfer_files(variable_files=variable_files,
               df=df,ssh_client=ssh_client,
               latest_date=latest_date,
               logger=logger,
               file_timestamp=file_timestamp)

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



transfer_df = tracker_df.loc[trf_df['read_status']==0,:]
for index, row in transfer_df.iterrows():
    data_to_database(timestamp=row['timestamp'],
                 file_path=row['file_path'],
                 db_connection=db_connection,
                 trf_df=trf_df,
                 variable_atts=variable_atts)
