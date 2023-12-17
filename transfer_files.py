import pandas as pd
import numpy as np
import os
import paramiko
from datetime import datetime
import shutil
from configs.paths import *


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

def choose_latest_date(ssh_client,logger,source_path,folder_format):
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
            



def transfer_files(ssh_client,variable_files:list,df:pd.DataFrame,latest_date:str,logger,file_timestamp:str):
    sftp_client = ssh_client.open_sftp()
    if os.path.exists(f'{destination_path}/{latest_date}')==False:
        os.makedirs(f'{destination_path}/{latest_date}')
        
    lst_folders = list(os.walk(f'{destination_path}'))[0][1]
    for x in lst_folders:
        if x != latest_date:
            print(f"Removing older folder {x}")
            shutil.rmtree(f'{destination_path}/{x}', ignore_errors=True)
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


