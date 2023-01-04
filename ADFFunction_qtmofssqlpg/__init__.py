# This function is a 'handler' to execute tasks initiated by a calling orchestrator function.  
# It is not intended to be invoked directly.

import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO,StringIO
import ssl
from urllib.parse import quote_plus
from decouple import config
import datetime
import sys
import json
"""
PostgreSQL Function calls post ADF completion:
Bi-Weekly Pipeline 
   process_l2r_lte_congestion
   process_l2r_nr_congestion
   process_l2r_magenta_build
   process_r2s_magenta_build
   process_agg_sector
   process_agg_rec

Monthly Pipeline
   process_l2r_hsi_sector
   process_l2r_smra
   process_r2s_building
   process_map_bldg_ookla
   process_agg_rec

run_function_once('dev.process_l2r_building', 1, engine)
run_function_once('dev.process_l2r_lte_congestion' 1, engine)
run_function_once('dev.process_l2r_nr_congestion', 1, engine)
run_function_once('dev.process_l2r_dnb_combined', 1, engine)
run_function_once('dev.process_l2r_engg_market_boundary', 1, engine)

run_function_once('dev.process_l2r_hsi_sector', 7, engine) # check

run_function_once('dev.process_l2r_magenta_build', 1, engine)
run_function_once('dev.process_l2r_ookla', 1, engine) # check
run_function_once('dev.process_l2r_smra_boundary', 1, engine)
run_function_once('dev.process_l2r_smra', 1, engine)
run_function_once('dev.process_l2r_starling_lte', 1, engine)
run_function_once('dev.process_l2r_starling_nr', 1, engine)
run_function_once('dev.process_r2s_magenta_build', 1, engine)
run_function_once('dev.process_r2s_ookla', 1, engine) # check

run_function_once('dev.process_agg_dnb', 1, engine)
run_function_once('dev.process_map_building', 1, engine)
run_function_once('dev.process_map_bldg_site', 1, engine)

run_function_iterations('dev.process_r2s_building', 50, engine)

run_function_once('dev.process_agg_sector', 1, engine)
"""
seq = log_df = engine = None

def main(name: str) -> str:
    global seq, log_df, engine 
    results = []
    connection = name['resource']
    username = connection['username']
    password = connection['password']
    host = connection['host']
    port = connection['port']
    database = connection['database']
    engine = ("postgresql://" + username + ":{0}@" + host + ":" + port + "/" + database).format(quote_plus(password))
    tasks = name['task_list']
    for task in tasks:
        seq = 0
        ct = datetime.datetime.now()
        log_init_data = [[seq, 'default', ct]]
        log_df = pd.DataFrame(log_init_data, columns=['Sequence', 'Function', 'Timestamp'])
        if (task['function_type'] == "run_function_once"):
            run_function_once(task['function_name'], 1, engine)
        elif (task['function_type'] == "run_function_iterations"):
            run_function_iterations(task['function_name'], int(task['parameters']['iterations']), engine)
        results.append(log_df.to_json())
    return results

def log_time(this_seq, this_func):
    global log_df
    new_row = pd.Series({'Sequence': this_seq, 'Function': this_func, 'Timestamp': datetime.datetime.now()})
    log_df = pd.concat([log_df, new_row.to_frame().T], ignore_index=True)
    return

def check_exit(func, cursor):
    try:
        sql_data = (func, )
        sql = "SELECT dev.check_graceful_exit(%s);"
        cursor.execute(sql, sql_data)
        exit_ind = cursor.fetchone()[0]
    except Exception as e:
        print(e)
        cursor.close()
        sys.exit("Cannot retrieve graceful exit")
    return exit_ind

def run_function_iterations(in_func, iterations, eng):
    global seq, log_df
    print('Begin ' + in_func)
    eng = create_engine(engine, isolation_level="AUTOCOMMIT")
    connection = eng.raw_connection()
    cursor = connection.cursor()
    print('Connection opened.')
    try:
        for x in range(iterations):
            if check_exit('default', cursor) == 'Y':
                print('Gracefully exiting...')
                break
            x = x + 1
            print(x)
            run_data = (in_func, x)
            run_query = "CALL dev.run_function(%s, %s);"
            seq = seq + 1
            log_time(seq, in_func+':'+str(iterations))
            cursor.execute(run_query, run_data)
            connection.commit()
            log_time(seq, in_func+':'+str(iterations))
            print('Commit successful.')
    except Exception as e:
        print(e)
        connection.rollback()
        print('Rollback successful.')
    cursor.close()
    print('Connection closed.')
    print('End '+ in_func)
    return

def run_function_once(in_func, iterations, eng):
    global seq, log_df
    print('Begin ' + in_func)
    eng = create_engine(engine, isolation_level="AUTOCOMMIT")
    connection = eng.raw_connection()
    cursor = connection.cursor()
    print('Connection opened.')
    if check_exit('default', cursor) == 'Y':
        print('Gracefully exiting...')
        cursor.close()
        print('Connection closed.')
        print('End '+ in_func)
        return
    try:
        run_data = (in_func, iterations)
        run_query = "CALL dev.run_function(%s, %s);"
        seq = seq + 1
        log_time(seq, in_func+':'+str(iterations))
        cursor.execute(run_query, run_data)
        log_time(seq, in_func+':'+str(iterations))
        connection.commit()
        print('Commit successful.')
    except Exception as e:
        print(e)
        connection.rollback()
        print('Rollback successful.')
    cursor.close()
    print('Connection closed.')
    print('End '+ in_func)
    return