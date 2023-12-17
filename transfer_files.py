import pandas as pd
import numpy as np
import os
import paramiko
from datetime import datetime

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
from configs.paths import *

# 20231208

folder_format = "%Y%m%d"
file_timestamp = folder_format+"T%H%M00Z"


# Get the logging csv
logger_path = 'logs/transfer.csv'
if os.path.exists(logger_path):
    df = pd.read_csv(logger_path)
else:
    df = pd.DataFrame({'timestamp':[],'variable':[],'status':[],'log_ts':[],'file':[],'read_status':[]})
    df.to_csv(logger_path,index=False)

def get_ssh():
    ssh = paramiko.SSHClient() ## Create the SSH object
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) # no known_hosts error
    try:
        ssh.connect(source_ip, username='ubuntu', key_filename=source_key)
    except Exception as e:
        print("There was an error")
        print(e)
    else:
        print("Connected Securely to the Source Server")
    return ssh





def choose_latest_date(ssh_client,logger=logger):
    stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}')
    date_folder_list = stdout.readlines()
    date_folder_list = [str(x)[:-1] for x in date_folder_list]
    date_folder_list.remove('EXIM')

    # converting to dates 
    
    date_folder_dates  = [datetime.strptime(x,folder_format) for x in date_folder_list]
    date_folder_dates.sort()
    latest_date = date_folder_dates[-1]

    # add to the logging 
    logger.info(f"Latest Date folder found is {latest_date}")

    choosing_latest =latest_date.strftime(folder_format)
    return choosing_latest



# read_variables = ['CMA','CT','CMIC','CRR','CRRPh','CTTH']
read_variables = ['CT',]


def seperate_files(files_list):
    list_nc_files = []
    for x in range(len(files_list)):
        ext = files_list[x].split('.')[1]
        if ext == 'nc':
            # print(ext)
            var_holder = files_list[x].split('_')[2]
            # print(var_holder)
            if var_holder in read_variables:
                list_nc_files.append(files_list[x])
    return list_nc_files
            



def transfer_files(ssh_client,variable_files:list,df:pd.DataFrame,latest_date:str,logger=logger):
    sftp_client = ssh_client.open_sftp()
    if os.path.exists(f'{destination_path}/{latest_date}')==False:
        os.makedirs(f'{destination_path}/{latest_date}')
    for x in variable_files:
        timstmp = datetime.strptime(x[:-3].split('_')[-1],file_timestamp)
        variable = 'CT'
        try:
            if os.path.exists(f'{destination_path}/{latest_date}/{x}')==False:
                sftp_client.get(f'{source_path}/{latest_date}/{x}',f'{destination_path}/{latest_date}/{x}')
                df = pd.concat([df,pd.DataFrame({'timestamp':[timstmp],'variable':[variable],'status':['transfered'],'log_ts':[datetime.now()],'file':[f'{x}'],'read_status':[0]})])
                logger.info(f"File transfered from source timestamp {timstmp} File Name {x}")
            else:
                # logger.warning(f"File {x} already exists")
                print(f"File {x} already exists")
        except Exception as e:
            print(e)
            df = pd.concat([df,pd.DataFrame({'timestamp':[timstmp],'variable':[variable],'status':[f'{e}'],'log_ts':[datetime.now()],'file':[f'{x}'],'read_status':[0]})])
    
    df.to_csv('logs/transfer.csv',index=False)



ssh_client = get_ssh()
latest_date = choose_latest_date(ssh_client=ssh_client)
# get all the variable files list in the latest date
stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}/{latest_date}')
variable_folders = stdout.readlines()
variable_folders = [str(x)[:-1] for x in variable_folders]
variable_files = seperate_files(variable_folders)
transfer_files(variable_files=variable_files,df=df,ssh_client=ssh_client,latest_date=latest_date)