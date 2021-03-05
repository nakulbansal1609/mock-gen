import pandas as pd, numpy as np
import random,requests
import string
from itertools import islice
import json
import pyodbc
import sqlalchemy
from sklearn.utils import shuffle
from sqlalchemy import insert, update
from sqlalchemy import MetaData, Table, create_engine
import base64

def engine_gen():
    return sqlalchemy.create_engine('mssql+pyodbc://{}:LOad@20201lowell@lwneusipoc001.public.15c3bf24e7be.database.windows.net,3342/lwneusidbpoc001?driver=ODBC Driver 17 for SQL Server'.format(db_user, db_pass))

def db_select(select):
    engine = engine_gen()
    df = pd.read_sql(select, con=engine)
    return df

def run_pyodbc_query(sql_query):
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+db_user+';PWD='+ db_pass)
    cnxn.execute(sql_query)
    cnxn.commit()
    cnxn.close()

def encode_base64(val):
    message_bytes = val.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    return base64_message

def decode_base64(val):
    base64_bytes = val.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode('ascii')
    return message

#### DB Params ####
server = 'lwneusipoc001.public.15c3bf24e7be.database.windows.net,3342'
database = 'lwneusidbpoc001'
db_user = ''
db_pass = ''
engine = engine_gen()
line_changes = random.randint(200,300)
table_list = pd.read_csv('table_gen_config.csv')

#### Read View ####
index_list = engine.execute('SELECT table_name, max_index FROM table_index_view_QA').fetchall()
for table in index_list:
    table_list.loc[table_list['Table'] == table[0],'next_index'] = table[1]+1

#### Inter ####
for table in table_list['Table']:
    line_changes = random.randint(200,300)
    print('Getting data for ', table)
    
    df = db_select('SELECT TOP({}) * FROM {} ORDER BY NEWID()'.format(line_changes,table))
    if(table_list.loc[table_list['Table']==table]['PK_Type'].to_string(index=False).strip() == 'int_base64'):
        #### Decoding ####
        df[table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip()] = [decode_base64(x) for x in df[table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip()]]
        #### Encoding ####
        #df[table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip()] = [encode_base64(x) for x in df[table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip()]]

    df_update = df.head(line_changes//2)
    exclude_cols = table_list.loc[table_list['Table']==table]['Exclude_Cols'].to_string(index=False).strip().split(',')
    exclude_cols = [x.strip() for x in exclude_cols]
    col_list = [x for x in list(df_update.columns) if x not in exclude_cols]
    update_col = random.choice(col_list)

    df_update[update_col]=shuffle(df[update_col]).reset_index(drop=True)
    df_insert = df.tail(line_changes-(line_changes//2)).reset_index(drop=True)
    df_insert[table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip()]=df_insert.index+int(round(float(table_list.loc[table_list['Table']==table]['next_index'].to_string(index=False).strip())))
    df_final = df_insert.append(df_update).reset_index(drop=True)

    if(table_list.loc[table_list['Table']==table]['PK_Type'].to_string(index=False).strip() == 'int_base64'):
        #### Encoding ####
        df_final[table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip()] = [encode_base64(str(x)) for x in df_final[table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip()]]
        df_final = df_final.drop_duplicates(subset=table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip(), keep="last")


    stage_table = '{}_stage'.format(table)
    try:
        engine.execute('DROP TABLE {}'.format(stage_table))
    except:
        pass
    create_table_sql = 'SELECT TOP(0) * INTO {} FROM {};'.format(stage_table,table)
    run_pyodbc_query(create_table_sql)
    try:
        df_final = df_final[df_final['KliNr']<=32000].reset_index(drop=True)
    except:
        pass

    df_final.to_sql(stage_table , con=engine, index=False, if_exists='append')
    if exclude_cols[0]=='NaN':
        join_cond = '(s.{}=t.{})'.format(table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip(), table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip())
    else:
        if(len(exclude_cols)>=1):
            join_cond = '(s.{}=t.{}) '.format(table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip(), table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip()) + ''.join(['AND (s.{}=t.{}) '.format(x,x) for x in exclude_cols])

    match_cond = 't.{}=s.{}'.format(update_col,update_col)
    not_match_cond = '({}) VALUES ({})'.format(', '.join(list(df_update.columns)) , ', '.join(['s.'+x for x in list(df_update.columns)]))

    sql_code= '''MERGE {} t
                USING {} s
                ON {}
                WHEN MATCHED
                   THEN UPDATE SET {}
                WHEN NOT MATCHED BY TARGET
                   THEN INSERT {};'''.format(table, stage_table, join_cond, match_cond, not_match_cond)
    run_pyodbc_query(sql_code)
    run_pyodbc_query('DROP TABLE {}'.format(stage_table))

    random_delete=random.randint(1,45)
    if exclude_cols[0]=='NaN':
        delete_query = 'DELETE FROM {table} WHERE {column} IN (SELECT TOP({random_delete}) {column} FROM {table})'.format(column = table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip(), table = table, random_delete = random_delete)
    else:
        delete_query = 'DELETE FROM {table} WHERE '.format(table = table) + ' AND '.join(['CONCAT({column},\'_\',{second_col}) IN (SELECT TOP({random_delete}) CONCAT({column},\'_\',{second_col}) FROM {table})'.format(column = table_list.loc[table_list['Table']==table]['PK'].to_string(index=False).strip(),random_delete = random_delete, table= table, second_col = x) for x in exclude_cols])
        
    run_pyodbc_query(delete_query)
    print('Stats for {table}.'.format(table=table))
    print('Rows inserted: {rows}.'.format(rows=line_changes))
    print('Rows deleted: {rows}.'.format(rows=random_delete))
    print('Done for {table}.'.format(table=table))
