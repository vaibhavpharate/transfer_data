from sqlalchemy import create_engine, text
from configs.db_conf import *

def get_connection(host,port,user,passord,database):
    connection_string = f"postgresql://{user}:{passord}@{host}/{database}"
    db_connect = create_engine(connection_string)
    try:
        with db_connect.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("\n\n---------------------Connection Successful")
        return db_connect
    except Exception as e:
        print("\n\n---------------------Connection Failed")
        print(f"{e}")
