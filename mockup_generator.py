import pandas as pd, numpy as np
import random,requests
from bs4 import BeautifulSoup
from faker import Faker
import rstr
import string
from itertools import islice
import hashlib
import re
import openpyxl
import json
import pyodbc
import sqlalchemy
#initialize Faker
fake=Faker()
import base64



def rand_no_gen(min_len, max_len):
    return int(rstr.rstr(string.digits, min_len, max_len))

def engine_gen():
    return sqlalchemy.create_engine('mssql+pyodbc://{}:{}@lwneusipoc001.public.15c3bf24e7be.database.windows.net,3342/lwneusidbpoc001?driver=ODBC Driver 17 for SQL Server'.format(db_user, db_pass))

def load_dict(dictionary_path):
    workbook = openpyxl.load_workbook(dictionary_path)
    sheet = workbook.active
    data = sheet.values
    cols = next(data)[1:]
    data = list(data)
    idx = [r[0] for r in data]
    data = (islice(r, 1, None) for r in data)
    df = pd.DataFrame(data, index=idx, columns=cols)
    return df

def generate_data(dictionary_path, table):
    df = load_dict(dictionary_path)
    df=df[df['Table']==table]

    df_out = pd.DataFrame(columns = list(df['column_name']))

def load_index():
    path = 'data_gen_index.json'
    with open(path) as json_file:
        data = json.load(json_file)
    return data


def update_index(index):
    path = 'data_gen_index.json'
    with open(path, 'w') as json_file:
        json.dump(index, json_file)

def db_select(select):
    engine = sqlalchemy.create_engine('mssql+pyodbc://{}:{}@lwneusqlpoc001.database.windows.net/lwneusdbpoc001?driver=ODBC Driver 17 for SQL Server'.format(db_user, db_pass))
    df = pd.read_sql(select, con=engine)
    return df

def load_db_index():
    df = db_select('SELECT * FROM table_index_view')
    return df

def generate_row(schema, columns, index=None):
    row = []
    for column_name in columns:
        column = schema[column_name]
        column['Type'] = column['Type'].lower()
        # res_generated = None
        # print(column_name)

        if random.choices([None,1],[column['% missing'],1-column['% missing']])[0]!=None:
            if column['Type']=='int':
                if '[' in str(column['Range']):
                    range = eval(column['Range'])
                    res_generated = random.randint(range[0], range[1])
                elif column['Range']=='constant':
                    res_generated = 12
                elif type(column['Range'])==int:
                    res_generated = column['Range']
                elif column['Range']=='unique':
                    res_generated = index
                else:
                    res_generated = random.randint(0,100000000000)
            elif column['Type']=='real_number':
                if '[' in str(column['Range']):
                    range = eval(column['Range'])
                    res_generated = round(random.uniform(range[0], range[1]),2)
                elif column['Range']=='constant':
                    res_generated = 12
                elif type(column['Range'])==float:
                    res_generated = column['Range']
                elif column['Range']=='unique':
                    res_generated = index
                else:
                    res_generated = round(random.uniform(0,100000000000),2)
            elif column['Type']=='int_base64':
                    res_generated = base64.b64encode(str(index).encode()).decode("utf-8")
            elif column['Type']=='categorical':
                try:
                    cat_number = random.randint(0, eval(column['Range'])-1)
                except:
                    cat_number = random.randint(0, int(column['Range'])-1)
                res_generated = re.sub('\d', '', hashlib.md5(bytes(str(cat_number), "ascii")).hexdigest())
            elif column['Type']=='null':
                res_generated = None
            elif column['Type']=='regex':
                res_generated = rstr.xeger(column['Range'])
            elif column['Type']=='bool':
                res_generated = random.choice([True, False])
            elif column['Type']=='person':
                res_generated = fake.name()
            elif column['Type']=='website':
                res_generated = fake.profile()['website'][0]
            elif column['Type']=='empty_str':
                res_generated = ''
            elif column['Type']=='currency':
                res_generated = fake.currency()[0]
            elif column['Type']=='int_skewed':
                range = eval(column['Range'])
                res_generated = random.randint(range[0], range[1])
            else:
                res_generated = eval('fake.{}()'.format(column['Type']))
        else:
            res_generated = None
        try:
            res_generated=str(res_generated[:int(column['CHARACTER_MAXIMUM_LENGTH'])])
        except Exception as e:
            #print(column['CHARACTER_MAXIMUM_LENGTH'], "| | ",res_generated, "| | ", print(e))
            pass
        row.append(res_generated)
    return row

def generate_table(data_dictionary, table, table_len, update_index_flg=True):
    df=data_dictionary[data_dictionary['Table']==table]
    df_out = pd.DataFrame(columns = list(df['column_name']))
    schema = df[['column_name','Type','Range','% missing','PK','CHARACTER_MAXIMUM_LENGTH']].set_index('column_name').T.to_dict('dict')
    columns = list(schema.keys())
    index = load_index()
    print(index)
    perc_list = {int(round((x/100)*table_len,0)):x for x in range(1, 100)}
    perc_vals = list(perc_list.keys())
    i = 0
    # if 'ref_int' in [x['Range'] for x in list(schema.values())]:
    #     db_index = load_db_index()
    # db_index['min_index']

    while i<=table_len:
        try:
            row = generate_row(schema, columns, index=index[table])
        except:
            index[table]=1
            row = generate_row(schema, columns, index=index[table])
        df_out = df_out.append(pd.Series(row, index = df_out.columns), ignore_index=True)
        i+=1
        index[table] = index[table]+1
        if i in perc_vals:
            print('Finished generating ', perc_list[i], '%')
    if update_index_flg==True:
        update_index(index)


    return df_out

db_user = 'lowelladmin'
db_pass = ''
engine = engine_gen()

#Set the file path for the schema
dictionary_path = 'nova_schema.xlsx'
data_dictionary = load_dict(dictionary_path)

#Set the number the rows that you want to be generated for each table
table_len = 2000

# Set the step lenght
step = 200

#Set the list of table you want to generate
table_list=['BI_Kampanj_Data_AktInfo',
                    'BI_Kampanj_Spc',
                    'LinKundNr',
                    'niAtgInf',
                    'niAtgTdo',
                    'niAtgTrd',
                    'niAtgVal',
                    'niAvsKod',
                    'niAvtKst',
                    'niGdPriv',
                    'niGldDbo',
                    'niGldSk',
                    'niKliRed',
                    'niKlikst',
                    'niKstDef',
                    'niMessage',
                    'niPartnr',
                    'niSakDat',
                    'niSkFakt',
                    'niSkKrfa',
                    'niSkMain',
                    'niTrsDat',
                    'niUTM',
                    'niVarDat',
                    'niVarInf',
                    'niAmoDat',
                    'niAvtDat',
                    'niAvtDat2',
                    'niGdAdr']

for table in table_list:
    i=0
    print('Generating data for: ', table)
    try:
        while i<=table_len:
            res = generate_table(data_dictionary, table, step, update_index_flg=True)
            engine = engine_gen()
            res.to_sql(table, index=False, con=engine, if_exists='append')
            i+=step
    except:
        print('Failed')
