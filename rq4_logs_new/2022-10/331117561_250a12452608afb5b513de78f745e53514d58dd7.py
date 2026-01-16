# -*- coding: utf-8 -*-
"""
Created on Fri May 20 11:12:40 2022

@author: MXS3524
"""
import pickle
import base64
import io
import os
import dash, dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input,Output,State
import plotly.express as px
import pandas as pd
from dash.dash import no_update
import dash_table
import datetime
from datetime import date
import dash_bootstrap_components as dbc
import re
import json
import webbrowser
from threading import Timer
import sqlite3
import plotly.graph_objects as go
import numpy as np
import pyodbc
from collections import OrderedDict
import openpyxl as xl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.dimensions import ColumnDimension
from dash.exceptions import PreventUpdate
from google.cloud.storage import Client, Blob, Bucket
from google.cloud import bigquery
import google.auth
data_bucket='common-code'
project_id='analytics-da-reporting-thd'
storage_client = Client(project=project_id)
bucket = Bucket(storage_client, data_bucket)
functions = bucket.get_blob('common_functions.py').download_as_string()
exec(functions)
narrative = bucket.get_blob('Narrative Functions.py').download_as_string()
exec(narrative)


##################################################################################################
##################################################################################################
########################################VARIABLES#################################################
##################################################################################################
##################################################################################################
# global dataframe_dict
parameters_dict={}
dataframe_dict={}
metadata_dict={}
currMetric=''
buttons_counter=1
port = 8050 # or simply open on the default `8050` port
css_button=	{'border-radius' : '15px', 'width' : '160px', 'backgroundColor' : 'orange'}
css_label={'width' : '250px', 'align' : 'right','fontWeight' : 'bold', 'verticalAlign' : 'center' , 'backgroundColor' : 'orange'}
css_legend={'backgroundColor' : 'gray', 'color' : 'white', 'padding' : '5px 10px', }
css_fieldset={'backgroundColor' : 'orange', 'border-radius' : '5px', 'border' : '1px solid black' }
filter_conditions=[]
filter_conditions.append('EQUALS')
filter_conditions.append('NOT EQUALS')
kwargs={
        'metricName' : 'OSHA_CNT_TOTAL',
        'column' : 'OSHA_REC_COUNT_VPLAN',
        'calculation' : None        
        }  

##################################################################################################
##################################################################################################
########################################HELPER FUNCTIONS##########################################
##################################################################################################
##################################################################################################
def write_variables(varName,varValue):
    username='mxs3524'
    filePath=r'C:\Users' + '\\' + username
    fileName=r'Testing.txt'
    file=os.path.join(filePath,fileName)
    val=varName + ':' + str(varValue)
    with open(file,'a') as f:
        f.write(val)
        f.write('\n')

def create_df_metadata():
    for df in dataframe_dict.keys():
        metadata_dict[df]=list(dataframe_dict[df].columns)
        
def saveData():
    pickle.dump(dataframe_dict, open(r'C:\Users\mxs3524\Downloads\Metadata', "wb"))

def loadData():
    global dataframe_dict,metadata_dict
    dataframe_dict = pickle.load(open(r'C:\Users\mxs3524\Downloads\Metadata', "rb"))        
    create_df_metadata()    
        
def open_browser():
	webbrowser.open_new("http://localhost:{}".format(port))

def dml_query(qry):
    credentials,prjt=google.auth.default()
    bq=bigquery.Client(project=project_id)
    job_config=bigquery.QueryJobConfig()
    job_config.use_legacy_sql=False
    insert_job=bq.query(qry,job_config=job_config)
    insert_job.result()
    if not qry.upper().startswith("CALL"):
        z=insert_job.num_dml_affected_rows
    else:    
        for result in insert_job.result():
            z=result[0]
    try:
        return z
    except:
        return 0

def merge_sql(**kwargs):
    merge_sql=f"MERGE `analytics-da-reporting-thd.NARRATIVE.{kwargs['table']}` AS TARGET USING (SELECT"
    # try:
    if 1==1:    
        for k,v in kwargs.items():
            if k not in ['dataset','table','pk']:
                if len(v)>0:
                    if  str(v[0]).endswith(")") or k.endswith("_NBR") or k in ['METRIC_RANK','ROUNDING']:
                        merge_sql+=f"{v[0]} AS {k},\n"
                    else:
                        merge_sql+=f"'{v[0]}' AS {k},\n"
                else:
                     merge_sql+=f"CAST(Null as String) AS {k},\n"
        merge_sql=merge_sql[0:-2]
        merge_sql+=") AS SOURCE \n ON 1=1"
        for k,v in kwargs.items():
            if k in ['pk']:
                # merge_sql+=f"\nAND TARGET.{v[0]}=SOURCE.{v[0]}"
                for x in v:
                    merge_sql+=f"\nAND TARGET.{x}=SOURCE.{x}"
        merge_sql+="\nWHEN MATCHED THEN UPDATE SET\n"
        for k,v in kwargs.items():
            if k not in ['dataset','table','pk']:
                if k!=kwargs['pk'][0]:
                    merge_sql+=f"{k}=SOURCE.{k},"
        merge_sql=merge_sql[0:-1]            
        merge_sql+="\nWHEN NOT MATCHED THEN \nINSERT VALUES ("
        for k,v in kwargs.items():
            if k not in ['dataset','table','pk']:
                merge_sql+=f'SOURCE.{k},'
        merge_sql=merge_sql[0:-1]        
        merge_sql+=');'
        z=dml_query(merge_sql)
        refresh_df_dictionary(kwargs['dataset'],kwargs['table'])
        saveData()
        loadData()
        msg= f"{z} record(s) were updated in the {kwargs['table']} table..."
        return z

 
def insert_sql(**kwargs):
    try:
        columns=[k for k in kwargs.keys() if k not in ['dataset','table','pk']]
        numRecords=len(kwargs[columns[0]])
        numColumns=len(columns)
    
        insert_sql=f"INSERT INTO `analytics-da-reporting-thd.NARRATIVE.{kwargs['table']}` ({','.join(columns)}) VALUES "
        for i in range(0,numRecords):
            insert_sql+='('
            for y in range(0,numColumns):
                insert_sql+=f"'{kwargs[columns[y]][i]}',"
            insert_sql=insert_sql[0:-1] 
            insert_sql+='),'
        insert_sql=insert_sql[0:-1]  
        z=dml_query(insert_sql)
        refresh_df_dictionary(kwargs['dataset'],kwargs['table'])
        saveData()
        loadData()
        return z
    except Exception as e:
       z=0
       return z

def delete_sql(**kwargs):
    numRows=0
    columns=[k for k in kwargs.keys() if k not in ['table','pk','dataset']]
    numRecords=len(kwargs[columns[0]])
    numColumns=len(columns)
    for i in range(0,numRecords):
        try:
            delete_sql=f"DELETE FROM `analytics-da-reporting-thd.NARRATIVE.{kwargs['table']}` WHERE 1=1"
            for y in range(0,numColumns):
                if columns[y].upper().endswith('_NBR') or kwargs[columns[y]][i].endswith('()'):
                    delete_sql+=f"\nAND {columns[y]}={kwargs[columns[y]][i]}"
                else:    
                    delete_sql+=f"\nAND {columns[y]}='{kwargs[columns[y]][i]}'"
            delete_sql+=';' 
            z=dml_query(delete_sql)
            numRows+=z
        except Exception as e:
            msg=f"There was an error: {str(e)}. {numRows} record(s) were deleted in the {kwargs['table']} table..."
            return msg
        
    refresh_df_dictionary(kwargs['dataset'],kwargs['table'])
    saveData()
    loadData()    
    msg= f"{numRows} record(s) were deleted in the {kwargs['table']} table..."    
    return msg 

def refresh_datasources(tableName):
    currTm=''
    sql_dict={
        'table' : 'METADATA',
        'pk' : ['DATA_SRC'],
        'LST_UPDT_TM' : [currTm],
         }
    return merge_sql(**sql_dict)  

def refresh_df_dictionary(dataset,tableName):
    global dataframe_dict
    sql=f"SELECT * FROM `{dataset}.{tableName}`"
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard')
    dataframe_dict[tableName]=df
    create_df_metadata()
    
def create_df_dictionary():
    client=bigquery.Client(project='analytics-da-reporting-thd')
    dataset_id='BLDG_SVCS_SERVICE_CHANNEL'
    dataset_id='NARRATIVE_V'
    tables=client.list_tables(dataset_id)
    views_list=[]
    
    for t in tables:
        if t.table_type=='VIEW':
            views_list.append(t.table_id)
    
    for v in views_list:
        if v not in dataframe_dict:
            print(f"Downloading {v}...")
            df=pd.read_gbq(f"SELECT * FROM `NARRATIVE_V.{v}`",project_id='analytics-da-reporting-thd',dialect='standard')
            dataframe_dict[v]=df    
    
def get_tables():
    global dataframe_dict        
    all_tables=[k for k in dataframe_dict.keys() if k not in ['DOCUMENT','METRIC','METRIC_FILTERS','METRIC_GRP','METRIC_SRC','METRIC_VAL','METADATA']]
    return [{'label' : n, 'value' : n} for n in all_tables]    
  

def get_documents():
    global dataframe_dict
    if 'DOCUMENT' in dataframe_dict:
        return [{'label' : n, 'value' : n} for n in list(dataframe_dict['DOCUMENT']['DOC_NM'].tolist())]
    else:
        return None    
    
def get_metrics():
  global dataframe_dict
  if 'METRIC' in dataframe_dict:
      dataframe_dict['METRIC'].sort_values(by=['METRIC_NM'],inplace=True)
      return [{'label' : n, 'value' : n} for n in list(dataframe_dict['METRIC']['METRIC_NM'].tolist())]
  else:
      return None

def get_projects():
    return [{'label' : n, 'value' : n} for n in ['analytics-da-reporting-thd']] 

def get_datasets():
    return [{'label' : n, 'value' : n} for n in ['NARRATIVE_V']]  

def get_groups(df):
    columns=['GROUPBY COLUMN']
    data=[]
    types_dict=dict(df.dtypes)
    for k in types_dict.keys():
        if types_dict[k].name=='object':
            data.append([k])
    df_columns=pd.DataFrame(columns=columns,data=data)
    df_columns.sort_values(by=['GROUPBY COLUMN'],ascending=True,inplace=True)
    df=df_columns.to_dict(orient='records')
    return df

def get_dimensions(df):
    columns=[]
    types_dict=dict(df.dtypes)
    for k in types_dict.keys():
        if types_dict[k].name=='object':
            columns.append([k])
    return columns        
    
def get_grouping_columns(metricName):
    global dataframe_dict
    try:
        columns=['COLUMN']
        data=[]
        df=dataframe_dict['METRIC_SRC']
        df=df[df['METRIC_NM']==metricName]
        sourceName=df['METRIC_TBL'].values[0]
        df=dataframe_dict[sourceName]
        types_dict=dict(df.dtypes)
        for k in types_dict.keys():
            if types_dict[k].name=='object':
                data.append([k])
        df_columns=pd.DataFrame(columns=columns,data=data)
        df_columns.sort_values(by=['COLUMN'],ascending=True,inplace=True)
        df=df_columns.to_dict(orient='records')
        return df  
    except:
        return None

def get_metric_columns(metricName):
    global dataframe_dict
    try:
        columns=['COLUMN']
        data=[]
        df=dataframe_dict['METRIC_SRC']
        df=df[df['METRIC_NM']==metricName]
        sourceName=df['METRIC_TBL'].values[0]
        df=dataframe_dict[sourceName]
        types_dict=dict(df.dtypes)
        for k in types_dict.keys():
            if types_dict[k].name!='object':
                data.append([k])
        df=pd.DataFrame(columns=columns,data=data)
        return df.to_dict('records')
    except:
        return None

def get_all_columns(metricName):
    df=dataframe_dict['METRIC_SRC'].copy()
    df=df[df['METRIC_NM']==metricName]
    if df.empty:
        return None
    else:
        sourceName=df['METRIC_TBL'].values[0]
        columns=metadata_dict[sourceName]
        columns.sort()
        return [{'label' : n, 'value' : n} for n in columns]

def get_grouping_column_filters(metricName):
    global dataframe_dict
    try:
        data=[]
        df=dataframe_dict['METRIC_SRC']
        df=df[df['METRIC_NM']==metricName]
        sourceName=df['METRIC_TBL'].values[0]
        df=dataframe_dict[sourceName]
        types_dict=dict(df.dtypes)
        for k in types_dict.keys():
            if types_dict[k].name=='object':
                data.append([k])
        data.sort()
        return [{'label' : n,'value' : n} for n in data]
    except:
        return None

def get_narrative_views(project,dataset):
    client=bigquery.Client(project=project)
    tables=client.list_tables(dataset)
    views_list=[]
    for t in tables:
        if t.table_type=='VIEW':
            views_list.append(t.table_id)
    return views_list      

def generate_dataframe(tableName):
    global dataframe_dict
    df=dataframe_dict[tableName].copy()
    columns=[{'name' : c, 'id' : c} for c in df.columns]
    data=df.to_dict('records')
    d=dash_table.DataTable(
    id='source_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'white'
    },
    columns=columns,
    data=data,
    page_size=10,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    )   
    dataframe_fieldset=html.Fieldset([
        html.Legend('Data Source'),
        d
        ])
    return dataframe_fieldset


def generate_filtered_dataframe(**kwargs):
    global dataframe_dict
    # df=dataframe_dict['METRIC_SRC'].copy()
    # df=df[df['METRIC_NM']==kwargs['METRIC_NAME']]
    # source=list(df['METRIC_TBL'].tolist())[0]
    df=dataframe_dict[kwargs['table']].copy()
    if kwargs['filters'] is None:
        pass
    else:
        for k,v in kwargs['filters'].items():
            df=df[df[k]==v]
    if kwargs['groupby']  is None:
        pass
    else:
        df=pd.DataFrame(df.groupby(kwargs['groupby']).sum())
        df.reset_index(inplace=True)
    columns=[{'name' : c, 'id' : c} for c in df.columns]
    data=df.to_dict('records')
    d=dash_table.DataTable(
    id='source_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'white'
    },
    columns=columns,
    data=data,
    page_size=10,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    )   
    dataframe_fieldset=html.Fieldset([
        html.Legend('Data Source'),
        d
        ])
    return dataframe_fieldset    
        
        
def process_metrics(df1):
    # return df1.to_dict('records')
    for index,row in df1.iterrows():

        for c in df1.columns:
                if df1[c].isnull().values.all():
                # if pd.isnull(row[c]):
                    parameters_dict[c]=None
                else:
                    parameters_dict[c]=row[c]

        if not parameters_dict['GROUP BY'] is None:
                pass
                # parameters_dict['GROUP BY']=ast.literal_eval(parameters_dict['GROUP BY']) 

        if not parameters_dict['TOP_N'] is None:
            #top_n=json.loads(parameters_dict['TOP_N'].replace("'",'"'))
            top_n=parameters_dict['TOP_N']
        else:
            top_n = None

        if not parameters_dict['AGGREGATE FILTERS']  is None:
            aggregate_filters=json.loads(parameters_dict['AGGREGATE FILTERS'].replace("'",'"'))
        else:
            aggregate_filters = None

        df=dataframe_dict[parameters_dict['VIEW']].copy(deep=True)

        if not parameters_dict['ROW FILTERS'] is None:
                df=apply_row_filters(df,parameters_dict['ROW FILTERS'])
        df=get_updated_dataframe(df.copy(deep=True),parameters_dict['COLUMN'],parameters_dict['CALCULATION'],top_n,aggregate_filters,parameters_dict['GROUP BY']) 
        if not aggregate_filters is None:
                df=apply_aggregate_filters(df,aggregate_filters)
        if not top_n is None:
            return get_top_n(df,parameters_dict['COLUMN'],parameters_dict['CALCULATION'],top_n)
        else:
            return get_calculation(df,parameters_dict['COLUMN'],parameters_dict['CALCULATION'],parameters_dict['AGGREGATE'])        
        
        
        
        
        
        
def calculate_value(**kwargs):
    sql="SELECT * FROM `NARRATIVE.VW_METRICS`"
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard')
    df=df[df['METRIC_NAME']==kwargs['metricName']]
    project=df['PROJECT'].values[0]
    dataset=df['DATASET'].values[0]
    view=df['VIEW'].values[0]
    filters=df.loc[:,['COLUMN_NAME','OPERATOR','COLUMN_VALUE']]
    filters.drop_duplicates(inplace=True)
    filters_dict={}
    for index,row in filters.iterrows():
        if row['OPERATOR']=='EQUALS':
            filters_dict.update({row['COLUMN_NAME'] : row['COLUMN_VALUE']})
    groupby=list(df['GROUPBY_COLUMN'].tolist())  
    columns=['PROJECT','DATASET','VIEW','METRIC_NM','COLUMN','CALCULATION','AGGREGATE','ROW FILTERS','AGGREGATE FILTERS','GROUP BY','TOP_N','FORMATS']
    data=[]
    data.append(project)
    data.append(dataset)
    data.append(view)
    data.append(kwargs['metricName'])
    data.append(df['COLUMN'].values[0])
    data.append(df['CALCULATION'].values[0])
    data.append(None)
    data.append(filters_dict)
    data.append(None)
    data.append(groupby)
    if 'RANK' in kwargs and 'ORDERBY' in kwargs and 'ASCENDING' in kwargs:
        data.append({'RANK' : kwargs['RANK'],'ORDER_BY' : kwargs['ORDERBY'], 'ASCENDING' : kwargs['ASCENDING']})
    else:
        data.append(None)
    data.append(None)
    df_metric=pd.DataFrame(columns=columns,data=[data])
    if not kwargs['column'] is None:
        df_metric['COLUMN']=kwargs['column']
    else:
        df_metric['CALCULATION']=kwargs['calculation']
    df_metric['ROW FILTERS']=df_metric['ROW FILTERS'].apply(lambda x : str(x))
    if df_metric['GROUP BY'][0][0] is None:
            df_metric['GROUP BY']=None
    # return str(df_metric.to_dict('records'))      
    return process_metrics(df_metric)  



def calculate_all_metrics(metricName):
    calc_dict,sql_dict,format_dict={},{},{}
    calc_dict.update({'metricName' : metricName})
    # sql="SELECT * FROM `NARRATIVE.METRIC_VAL`"
    sql='SELECT distinct * except (COLUMN_NAME,OPERATOR,COLUMN_VALUE) FROM `analytics-da-reporting-thd.NARRATIVE.VW_METRICS`'
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard')
    df=df[(~pd.isnull(df['METRIC_COL'])) | (~pd.isnull(df['METRIC_CALC']))]
    df=df[df['METRIC_NAME']==metricName]
    if df.empty:
        return 0
    
    if df['METRIC_COL'].isnull().values.all():    
            calc_dict.update({'column' : None})
            calc_dict.update({'calculation' : df['METRIC_CALC'].values[0]})
    else:
            calc_dict.update({'calculation' : None})
            calc_dict.update({'column' : df['METRIC_COL'].values[0]})
            
            
    if not df['METRIC_RANK'].isnull().values.all():  
         calc_dict.update({'RANK' : df['METRIC_RANK'].values[0]})
         calc_dict.update({'ORDERBY' : df['METRIC_ORDER_BY'].values[0]})
         calc_dict.update({'ASCENDING' : df['METRIC_ASCENDING'].values[0]})
            
            
            
    # try:
    if 1==1:    
        calc=calculate_value(**calc_dict)  
        calc=str(calc)
        format_dict['TYPE']=df['FORMAT_TYPE'].values[0]
        format_dict['ROUNDING']=df['ROUNDING'].values[0]
        format_dict['UNITS']=df['UNITS'].values[0]
        format_dict['CURRENCY']=df['CURRENCY'].values[0]
        calc_formatted=assign_formatted_values(calc,**format_dict)
    # except:
    #     calc='N/A'
    #     calc_formatted='N/A'
        
    sql_dict['dataset']='NARRATIVE'
    sql_dict['table']='METRIC_VALUES'
    sql_dict['pk']=['METRIC_NM']
    sql_dict['METRIC_NM']=[metricName]
    sql_dict['METRIC_VALUE']= [calc]
    sql_dict['METRIC_FORMATTED']=[calc_formatted]
    sql_dict['LAST_UPD_TM']= ['current_datetime()']
    z=merge_sql(**sql_dict) 
    return z
    

def get_datasource_metadata():
    sql='''    
    SELECT Distinct concat(a.METRIC_PROJ,'.',a.METRIC_DSET,'.',a.METRIC_TBL) DATA_SOURCE, b.LST_UPDT_TM
    FROM `NARRATIVE.METRIC_SRC` a
    LEFT JOIN `analytics-da-reporting-thd.NARRATIVE.METADATA` b
    on concat(a.METRIC_PROJ,'.',a.METRIC_DSET,'.',a.METRIC_TBL) =b.DATA_SRC
    ORDER BY 1
    '''
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard')
    df.rename(inplace=True,columns={'DATA_SOURCE' : 'DATA SOURCE', 'LST_UPDT_TM' : 'LAST UPDATED'})    
    return df.to_dict('records')

    
def get_metric_values_metadata():
    sql='''
    SELECT a.METRIC_NM, b.METRIC_VALUE,b.METRIC_FORMATTED,b.LAST_UPD_TM
    FROM `NARRATIVE.METRIC` a
    LEFT JOIN `analytics-da-reporting-thd.NARRATIVE.METRIC_VALUES` b
    on a.METRIC_NM=b.METRIC_NM
    ORDER BY 1
    '''
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard')
    df.rename(inplace=True,columns={'METRIC_NM' : 'METRIC', 'METRIC_VALUE' : 'VALUE','METRIC_FORMATTED' : 'FORMATTED','LAST_UPD_TM' : 'LAST UPDATED'})
    return df.to_dict('records')
    

def assign_formatted_values(metricValue,**format_dict):
    divisors_dict={}
    divisors_dict['M']=1000000
    divisors_dict['B']=1000000000
    divisors_dict['T']=1000
    divisors_dict['None']=1
    format_dict['ROUNDING']=int(str(format_dict['ROUNDING']).replace(".0","")) 
    
    if format_dict['TYPE']=='STRING':
        return str(metricValue)
        
    elif format_dict['TYPE'] =='FLOAT':
        f=float(metricValue)
        places=format_dict['ROUNDING']
        divisor=divisors_dict[format_dict['UNITS']]
        if 'CURRENCY' in format_dict.keys():
            currency_str="${:,." + str(places) + "f}"
        elif 'CURRENCY' not in format_dict.keys():
            currency_str="{:,." + str(places) + "f}"
        if 'UNITS' in format_dict.keys():
            if format_dict['UNITS'].upper()!='NONE':
                currency=currency_str.format(f/divisor) + format_dict['UNITS']
            else:
                currency=currency_str.format(f/divisor)
        elif 'UNITS' not in format_dict.keys():
            currency=currency_str.format(f/divisor)
        return currency  
    
    elif format_dict['TYPE']=='INTEGER':
        f=float(metricValue)
        places=format_dict['ROUNDING']
        divisor=divisors_dict[format_dict['UNITS']]
        if 'CURRENCY' in format_dict.keys():
            currency_str= "${:,.0f}"
        elif 'CURRENCY' not in format_dict.keys():
            currency_str= "{:,.0f}"
        if 'UNITS' in format_dict.keys():
            if format_dict['UNITS'].upper()!='NONE':
                currency=currency_str.format(int(f/divisor)) + format_dict['UNITS']
            else:
                currency=currency_str.format(int(f/divisor))
        elif 'UNITS' not in format_dict.keys():
            currency=currency_str.format(int(f/divisor))
        return currency  
    
    elif format_dict['TYPE'] =='PERCENT':
        f=float(metricValue)
        places=format_dict['ROUNDING']
        currency_str="{:+." + str(places) + "%}"
        currency=currency_str.format(f) 
        return currency
        
        
        
def update_copied_metric(metricName,metricCopy):
    cnt=0
    sql=f'''
    MERGE INTO `NARRATIVE.METRIC_VAL` target
    USING (
    SELECT 
    '{metricCopy}' METRIC_NM,
    METRIC_COL,
    METRIC_CALC
    FROM `NARRATIVE.METRIC_VAL`
    WHERE METRIC_NM='{metricName}') source
    ON target.METRIC_NM=source.METRIC_NM
    WHEN MATCHED THEN UPDATE
    SET
    METRIC_COL=source.METRIC_COL,
    METRIC_CALC=source.METRIC_CALC
    WHEN NOT MATCHED THEN INSERT VALUES
    ('{metricCopy}',source.METRIC_COL,source.METRIC_CALC)
    '''
    z=dml_query(sql)
    cnt+=z
    sql=f'''
    MERGE INTO `NARRATIVE.METRIC_TOP_N` target
    USING (
    SELECT 
    '{metricCopy}' METRIC_NM,
    METRIC_RANK,
    METRIC_ASCENDING,
    METRIC_ORDER_BY
    FROM `NARRATIVE.METRIC_TOP_N`
    WHERE METRIC_NM='{metricName}') source
    ON target.METRIC_NM=source.METRIC_NM
    WHEN MATCHED THEN UPDATE
    SET
    METRIC_RANK=source.METRIC_RANK,
    METRIC_ASCENDING=source.METRIC_ASCENDING,
    METRIC_ORDER_BY=source.METRIC_ORDER_BY
    WHEN NOT MATCHED THEN INSERT VALUES
    ('{metricCopy}',source.METRIC_RANK,source.METRIC_ASCENDING,source.METRIC_ORDER_BY)
    '''
    z=dml_query(sql)
    cnt+=z
    sql=f'''
    MERGE INTO `NARRATIVE.METRIC_FORMATTING` target
    USING (
    SELECT 
    '{metricCopy}' METRIC_NM,
    FORMAT_TYPE,
    ROUNDING,
    UNITS,
    CURRENCY
    FROM `NARRATIVE.METRIC_FORMATTING`
    WHERE METRIC_NM='{metricName}') source
    ON target.METRIC_NM=source.METRIC_NM
    WHEN MATCHED THEN UPDATE
    SET
    FORMAT_TYPE=source.FORMAT_TYPE,
    ROUNDING=source.ROUNDING,
    UNITS=source.UNITS,
    CURRENCY=source.CURRENCY
    WHEN NOT MATCHED THEN INSERT VALUES
    ('{metricCopy}',source.FORMAT_TYPE,source.ROUNDING,source.UNITS,source.CURRENCY)
    '''
    z=dml_query(sql)
    cnt+=z
    return cnt    
        
        
def get_saved_verbiage(docName):
    sql="SELECT * FROM `NARRATIVE.METRIC_VERBIAGE` ORDER BY PARAGRAPH_NBR,SENTENCE_NBR"  
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard') 
    df=df[df['DOCUMENT_NM']==docName]     
    columns={'PARAGRAPH_NBR' : 'PARAGRAPH','SENTENCE_NBR' : 'SENTENCE', 'NEW_LINE' : 'NEW LINE', 'ALIGNMENT' : 'JUSTIFY'}
    df.rename(columns=columns,inplace=True)
    df=df.loc[:,['PARAGRAPH','SENTENCE','VERBIAGE','PARAMETERS','NEW LINE','JUSTIFY','BULLETS']]
    return df.to_dict('records')


def get_verbiage_output(docName):
    sql="SELECT * FROM `NARRATIVE.VERBIAGE_OUTPUT` ORDER BY PARAGRAPH_NBR,SENTENCE_NBR"  
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard') 
    df=df[df['DOC_NM']==docName]     
    columns={'PARAGRAPH_NBR' : 'PARAGRAPH','SENTENCE_NBR' : 'SENTENCE', 'LAST_UPD_TM' : 'LAST UPDATED','OUTPUT_VERBIAGE' : 'OUTPUT'}
    df.rename(columns=columns,inplace=True)
    df=df.loc[:,['PARAGRAPH','SENTENCE','OUTPUT','LAST UPDATED']]
    return df.to_dict('records')


def update_output(docName,paragraphNbr,sentenceNbr):
    sql="SELECT * FROM `NARRATIVE.METRIC_VERBIAGE` ORDER BY PARAGRAPH_NBR,SENTENCE_NBR"  
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard') 
    df=df[df['DOCUMENT_NM']==docName]  
    df=df[df['PARAGRAPH_NBR']==paragraphNbr]
    df=df[df['SENTENCE_NBR']==sentenceNbr]
    raw_verbiage=df['VERBIAGE'].values[0]
    
    sql="SELECT * FROM `NARRATIVE.METRIC_VERBIAGE` ORDER BY PARAGRAPH_NBR,SENTENCE_NBR"  
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard') 
    
   
    
def assign_verbiage_output(docName,paragraphNbr,sentenceNbr):
    sql="SELECT * FROM `NARRATIVE.VW_METRIC_VERBIAGE`"
    df=pd.read_gbq(sql,project_id='analytics-da-reporting-thd',dialect='standard')
    df=df[df['document_nm']==docName]
    df=df[df['paragraph_nbr']==paragraphNbr]
    df=df[df['sentence_nbr']==sentenceNbr]
    dt=min(list(df['LAST_UPD_TM'].tolist()))
    verbiage=df['verbiage'].values[0]
    parameters=df['parameters'].values[0]
    parameters=re.findall('\w+',parameters)
    parameters_dict=pd.Series(df.METRIC_FORMATTED.values,index=df.METRIC_NM).to_dict()
    parameters=[parameters_dict[x] for x in parameters]
    while True:
        if verbiage.find("?")==-1:
            break
        else:
            verbiage=verbiage.replace("?",parameters.pop(0),1)
    sql_dict={}
    sql_dict['dataset']='NARRATIVE'
    sql_dict['table']='VERBIAGE_OUTPUT'
    sql_dict['pk']=['DOC_NM','PARAGRAPH_NBR','SENTENCE_NBR']
    sql_dict['DOC_NM']=[docName]
    sql_dict['PARAGRAPH_NBR']=[paragraphNbr]
    sql_dict['SENTENCE_NBR']=[sentenceNbr]
    sql_dict['LAST_UPD_TM']=[f"DATETIME('{dt}')"]
    sql_dict['OUTPUT_VERBIAGE']=[verbiage]
    z=merge_sql(**sql_dict)        
    return z    
    


#################################################################################################################
#################################################################################################################
######################################LAYOUT FUNCTIONS###########################################################
#################################################################################################################
#################################################################################################################
def metric_layout():
    metric_layout=html.Fieldset([
         dbc.InputGroup([
            dcc.Checklist(id='checkbox_copy_metric',options=[{'label' :  v, 'value' : v}for v in ['Copy Existing Metric:']],style=css_label),
            dbc.Select(options=metrics,id='select_metric_copy',disabled=True)
            ]),
        
        html.Legend('METRIC'),
        dbc.InputGroup([
            dbc.InputGroupText('METRIC NAME:',style=css_label),
            dbc.Input(type='text',id='text_metric_nm',debounce=True),
            ]),
        dbc.InputGroup([
            dbc.InputGroupText('METRIC DESCRIPTION:',style=css_label),
            dbc.Input(type='text',id='text_metric_desc'),
            ]),
        dbc.InputGroup([
            dbc.InputGroupText('DOCUMENT NAME:',style=css_label),
            dbc.Select(options=get_documents(),id='text_metric_doc')
            ]),
 
        # html.Br(),
        # html.Button("SAVE METRIC",style=css_button,id='btn_save_metric')
         ])
    return metric_layout

def document_layout():
    document_layout=html.Fieldset([
        html.Legend('DOCUMENTS'),
        dbc.InputGroup([
            dbc.InputGroupText('DOCUMENT NAME:',style=css_label),
            dbc.Input(type='text',id='text_doc_nm'),
            ]),
        dbc.InputGroup([
            dbc.InputGroupText('DOCUMENT DESCRIPTION:',style=css_label),
            dbc.Input(type='text',id='text_doc_desc'),
            ]),
        
        html.Br(),
        html.Button("SAVE DOCUMENT",style=css_button,id='btn_save_document')
         ])
    return document_layout

def source_layout():
    source_layout=html.Fieldset([
        html.Legend('SOURCE'),
        # dbc.InputGroup([
        #     dbc.InputGroupText('METRIC NAME:',style=css_label),
        #     dbc.Select(options=metrics,id='src_metric_nm'),
        #     ]),
        dbc.InputGroup([
            dbc.InputGroupText('PROJECT:',style=css_label),
            dbc.Select(options=get_projects(),id='src_project_nm'),
            ]),
        dbc.InputGroup([
            dbc.InputGroupText('DATASET:',style=css_label),
            dbc.Select(options=get_datasets(),id='src_dataset_nm')
            ]),
        dbc.InputGroup([
            dbc.InputGroupText('TABLE:',style=css_label),
            dbc.Select(options=get_tables(),id='src_table_nm')
            ]),
        
                
        # html.Br(),
        # html.Button("SAVE SOURCE",style=css_button,id='btn_save_source')
         ])
    return source_layout

def filter_layout():
    columns=[{'name' : c, 'id' : c} for c in ['COLUMN','OPERATOR','VALUE']]
    d=dash_table.DataTable(
    id='filters_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'orange'
    },
    columns=columns,
    # data=[{'COLUMN' : '','OPERATOR' : '','VALUE' : ''}],
    # data=df.to_dict('records'),
    row_selectable='multi',
    # sort_action='native',
    page_size=5,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    # style_data={'backgroundColor' : 'lightblue'},
    )
   
    filter_layout=html.Div([
    html.Fieldset([
    html.Legend('FILTERS'),
    # dbc.InputGroup([
    #     dbc.InputGroupText('METRIC NAME:',style=css_label),
    #     dbc.Select(options=metrics,id='filter_metric_nm'),
    #     ]),
    dbc.InputGroup([
        dbc.InputGroupText('COLUMN:',style=css_label),
        dbc.Select(options=[],id='filter_column_nm'),
        ]),
    dbc.InputGroup([
        dbc.InputGroupText('OPERATOR:',style=css_label),
        dbc.Select(options=[{'label' : n, 'value' : n} for n in filter_conditions],id='filter_operator')
        ]),
    dbc.InputGroup([
        dbc.InputGroupText('VALUE:',style=css_label),
        dbc.Select(options=[],id='filter_val_nm')
        ]),
    html.Br(),
    html.Button("ADD FILTER",style=css_button,id='btn_add_filter')
    ]),
    html.Br(),html.Br(),
    d,
    html.Br(),
    # html.Button('VIEW DATA',id='btn_refresh_dataframe',style=css_button),
    # html.Button('SAVE FILTER',id='btn_save_filter',style=css_button),
    html.Button('REMOVE FILTER',id='btn_delete_filter',style=css_button),
    html.Br(),html.Br(),
    ])
        
    return filter_layout

def grouping_layout():
    columns=[{'name' : c, 'id' : c} for c in ['COLUMN']]
    d=dash_table.DataTable(
    id='grouping_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'orange'
    },
    columns=columns,
    row_selectable='multi',
    page_size=10,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    # style_data={'backgroundColor' : 'lightblue'},
    )
   
    grouping_layout=html.Div([
    html.Fieldset([
    html.Legend('GROUPING'),
    # dbc.InputGroup([
    #     dbc.InputGroupText('METRIC NAME:',style=css_label),
    #     dbc.Select(options=metrics,id='grp_metric_nm'),
    #     ]),
    ]),
    d,
    # html.Br(),html.Br(),
    # html.Button("SAVE GROUP BY",style=css_button,id='btn_save_grouping')
    ])
        
    return grouping_layout

def calculation_layout():
    columns=[{'name' : c, 'id' : c} for c in ['COLUMN']]
    d=dash_table.DataTable(
    id='available_columns_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'white'
    },
    columns=columns,
    row_selectable='single',
    page_size=10,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    )
    calculation_layout=html.Div([
    html.Fieldset([
    html.Legend('CALCULATION'),
    dbc.InputGroup([
        dbc.InputGroupText('METRIC NAME:',style=css_label),
        dbc.Select(options=metrics,id='calc_metric_nm'),
        ]),
    dbc.InputGroup([
        dbc.InputGroupText('METRIC TYPE:',style=css_label),
        dbc.Select(options=[{'label' : n, 'value' : n} for n in ['COLUMN','CALCULATION']],id='calc_metric_type'),
        ]), 
    html.Br(),
    dbc.Row([
    dbc.Col([
        html.Div([
        dbc.InputGroup([
        dbc.InputGroupText('COLUMN:',style=css_label),
        dbc.Select(options=[],id='calc_metric_column'),
        ]), 
          ],id='column_value'),
        html.Div([
        dbc.InputGroup([
        dbc.InputGroupText('CALCULATION:',style=css_label),
        dbc.Input(type='text',id='metric_calculation'),
        ]),   
        ],id='calculation_value')
        ])  #col
        ]), #row
  
    #put dash table here with all available fields
    html.Br(),
    html.Div([
    dbc.InputGroup([
    dcc.Checklist(id='checkbox_topn',options=[{'label' : n, 'value' : n} for n in ['Apply Top N?']])    
    ]),
    
    dbc.InputGroup([
    dbc.InputGroupText('RANK:',style=css_label),
    dbc.Select(options=[{'label' : n, 'value' : n} for n in range(1,11)],id='select_rank')
    ]),
    
    dbc.InputGroup([
    dbc.InputGroupText('ORDER BY:',style=css_label),
    dbc.Input(type='text',id='text_order_by')
    ]),
    
    dbc.InputGroup([
    dbc.InputGroupText('ASCENDING',style=css_label),
    dbc.Select(options=[{'label' : n, 'value' : n} for n in ['TRUE','FALSE']],id='select_ascending')    
    ]),
    
    html.Br(),html.Br(),html.Br(),
    d,
    
    html.Br(),
    html.Button("CALCULATION",id='btn_add_column',style=css_button),
    html.Button("ORDER BY",id='btn_add_orderby',style=css_button),
    html.Br(),html.Br(),
    ]),
    ]),
    ])
    return calculation_layout


def calculated_layout():
    calculated_layout=html.Div([
    html.Fieldset([ 
    html.Legend('RESULTS'),  
    
    dbc.InputGroup([
        dbc.InputGroupText('RAW VALUE',style=css_label),
        dbc.Input(type='text',id='text_metric_value'),
        ]),
    
    
    dbc.InputGroup([
        dbc.InputGroupText('FORMATTED VALUE',style=css_label),
        dbc.Input(type='text',id='text_metric_formatted'),
        ]),
    
           
    
    html.Br(),
    html.Button('CALCULATE',id='btn_calculate_metric',style=css_button),
    html.Button('SAVE',id='btn_save_calculation',style=css_button)
    ])   
    ])
    return calculated_layout


def formatting_layout():
    formatting_layout=html.Div([
    html.Fieldset([
    html.Legend('FORMATTING'),
    dbc.InputGroup([
    dbc.InputGroupText('FORMAT TYPE:',style=css_label),
    dbc.Select(options=[{'label' : n, 'value' : n} for n in ['FLOAT','INTEGER','PERCENT','STRING']],id='select_format_type')    
    ]),    
    dbc.InputGroup([
    dbc.InputGroupText('ROUNDING:',style=css_label),
    dbc.Input(type='text',placeholder='number of decimal places',id='text_rounding')   
    ]),   
    dbc.InputGroup([
    dbc.InputGroupText('UNITS:',style=css_label),
    dbc.Select(options=[{'label' : n, 'value' : n} for n in ['None','T','M','B']],id='select_units')    
    ]),
    dbc.InputGroup([
    dcc.Checklist(id='checkbox_currency',options=[{'label' : n, 'value' : n} for n in ['Apply Currency?']])    
    ]),            
    ],style=None)    
    ])
    return formatting_layout





def values_layout():
    columns=[{'name' : c, 'id' : c} for c in ['METRIC','VALUE','FORMATTED','LAST UPDATED']]
    d=dash_table.DataTable(
    id='metric_values_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'white'
    },
    columns=columns,
    data=calculation_metadata,
    row_selectable='multi',
    page_size=10,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    )
    values_layout=html.Div([
    html.Fieldset([
    html.Legend('METRIC VALUES'), 
    d,
    html.Br(),
    html.Button('REFRESH',id='btn_refresh_metric_values',style=css_button),  
    html.Button('REFRESH ALL',id='btn_refresh_metric_values_all',style=css_button)
    ]),
  
    ])
    return values_layout                     





def metadata_layout():
    columns=[{'name' : c, 'id' : c} for c in ['DATA SOURCE','LAST REFRESHED']]
    d=dash_table.DataTable(
    id='metadata_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'orange'
    },
    columns=columns,
    # data=[{'COLUMN' : '','OPERATOR' : '','VALUE' : ''}],
    # data=df.to_dict('records'),
    row_selectable='multi',
    sort_action='native',
    page_size=10,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    # style_data={'backgroundColor' : 'lightblue'},
    )
    metadata_layout=html.Div([
    html.Fieldset([
    html.Legend('METADATA'),
    d,
    html.Br(),
    html.Button('REFRESH',id='btn_refresh_metadata',style=css_button),
    html.Button('REFRESH ALL',id='btn_refresh_metadata_all',style=css_button),
    html.Br(),html.Br(),
    dbc.InputGroup([
        dbc.InputGroupText('Pickle File Location'),
        dbc.Input(type='text',id='text_pickle_location')
        ])
    ]),
    ])
    return metadata_layout



def verbiage_output_layout():
    columns=[{'name' : n, 'id' : n} for n in ['PARAGRAPH','SENTENCE','OUTPUT','LAST UPDATED']]
    v=dash_table.DataTable(
    id='verbiage_output_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'white'
    },
    columns=columns,
    row_selectable='single',
    sort_action='native',
    page_size=5,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    )
    return html.Div([
        html.Fieldset([
        html.Legend('Verbiage Output'),    
        dbc.InputGroup([
        dbc.InputGroupText('DOCUMENT NAME:',style=css_label),
        dbc.Select(options=get_documents(),id='select_metadata_doc')    
        ]),  
        v,
        html.Br(),
        html.Button('REFRESH',id='btn_refresh_verbiage',style=css_button),
        html.Button('REFRESH ALL',id='btn_refresh_all_verbiage',style=css_button)
        ])
        ])


def verbiage_data_layout():
    global verbiage
    columns=[{'name' : c, 'id' : c, 'editable' : True if c not in ['PARAGRAPH','SENTENCE'] else False, 'presentation' : 'dropdown' if c in ['NEW LINE','JUSTIFY','BULLETS'] else None} for c in ['PARAGRAPH','SENTENCE','VERBIAGE','PARAMETERS','NEW LINE','JUSTIFY','BULLETS']]
    col_list=['TRUE','FALSE']
    v=dash_table.DataTable(
    id='verbiage_saved_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'white'
    },
    columns=columns,
    # data=verbiage,
    row_selectable='single',
    sort_action='native',
    page_size=5,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    # style_data={'backgroundColor' : 'lightblue'},
    
    dropdown={
        
    'NEW LINE': {
                'options': [
                    {'label': i, 'value': i}
                    for i in col_list  
                ]},
            
    'JUSTIFY': {
                  'options': [
                    {'label': i, 'value': i}
                    for i in col_list    
                ]},
    
    
    'BULLETS': {
                  'options': [
                    {'label': i, 'value': i}
                    for i in col_list    
                ]},
    }   , 
    
    )
    return html.Div([
        html.Fieldset([
        html.Legend('Saved Verbiage'),    
        v,
        html.Br(),
        html.Button('DELETE',id='btn_delete_verbiage',style=css_button),
        html.Button('UPDATE',id='btn_update_verbiage',style=css_button)
        ])
        ])


def verbiage_layout():
    global metrics
    d=dash_table.DataTable(
    id='verbiage_metrics_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'white'
    },
    columns=[{'name' : c, 'id' : c} for c in ['METRIC']],
    row_selectable='multi',
    sort_action='native',
    page_size=5,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    # style_data={'backgroundColor' : 'lightblue'},
    )    
    
    verbiage_layout=html.Div([
    html.Fieldset([
    html.Legend('Create Verbiage'),
    dbc.InputGroup([
    dbc.InputGroupText('Document Name:',style=css_label),
    dbc.Select(id='verbiage_doc',options=get_documents())        
    ]),
    
    dbc.InputGroup([
    dbc.InputGroupText('Paragraph Number:',style=css_label),
    dbc.Input(type='text',id='verbiage_paragraph')        
    ]),
    
    dbc.InputGroup([
    dbc.InputGroupText('Sentence Number:',style=css_label),
    dbc.Input(type='text',id='verbiage_sentence')        
    ]),
       
    dbc.InputGroup([
    dbc.InputGroupText('Verbiage',style=css_label),
    dbc.Input(type='text',id='text_raw_verbiage',size=100)
    ]),
    ]),
     dbc.InputGroup([
    dcc.Checklist(id='checkbox_verbiage_attributes',options=[{'label' : n, 'value' : n} for n in ['New Line','Justify','Bullets']],style={'display' : 'inline-block'})    
    ]), 
    
    
   
    html.Fieldset([
    html.Legend('Assign Metrics'),    
    dbc.InputGroup([
    dbc.InputGroupText('METRIC NAME',style=css_label),
    dbc.Select(id='verbiage_metric_name',options=metrics)
    ]),
    html.Br(),
    html.Div([d],id='div_verbiage_metrics',),
    html.Br(),
    html.Button('ADD METRIC',id='btn_add_verbiage_metric',style=css_button),
    html.Button('DELETE',id='btn_delete_verbiage_metric',style=css_button)
    ]),
    html.Br(),html.Br(),
    html.Fieldset([
    html.Legend('Preview Verbiage'),    
    dbc.InputGroup([
    dbc.InputGroupText('Processed Verbiage',style=css_label),
    dbc.Input(id='text_processed_verbiage',type='text',size=100)
    ]),
    html.Br(),
    html.Button('PROCESS VERBIAGE',id='btn_create_processed_verbiage',style=css_button),
    html.Button('SAVE VERBIAGE',id='btn_save_processed_verbiage',style=css_button),
    html.Br(),html.Br(),
    # html.Div([d],id='div_verbiage_metrics',),
    # html.Br(),html.Br(),
    # html.Button('DELETE',id='btn_delete_verbiage_metric',style=css_button)
    ])
    # dbc.InputGroup([
    # dbc.InputGroupText('Preview Verbiage',style=css_label),
    # dbc.Input(type='text',id='text_preview_verbiage',size=100)
    # ]),
    ])
    return verbiage_layout




def data_sources_layout():
    columns=[{'name' : c, 'id' : c} for c in ['DATA SOURCE','LAST UPDATED']]
    d=dash_table.DataTable(
    id='data_sources_table',
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        'lineHeight': '15px',
        'backgroundColor' : 'white'
    },
    columns=columns,
    data=source_metadata,
    row_selectable='multi',
    page_size=10,
    style_cell={'textAlign': 'center'},
    style_header={'backgroundColor' : 'orange'},
    )
    data_sources_layout=html.Div([
    html.Fieldset([
    html.Legend('DATA SOURCES'),
    d,
    html.Br(),
    html.Button('REFRESH',id='btn_refresh_datasources',style=css_button),
    html.Button('REFRESH ALL',id='btn_refresh_datasources_all',style=css_button)
    ]),
    html.Div(id='div_datasource_status')
    ])
    return data_sources_layout
################################################################################################################################
################################################################################################################################
#######################################################APP LAYOUT###############################################################
################################################################################################################################
################################################################################################################################
def create_tab1_schema():
    section=dbc.Container([
    dbc.Row([
        dbc.Col([
        metric_layout(),
        html.Div(id='div_metric_status'), 
        ]),
        
        dbc.Col([
        source_layout(),
        html.Div(id='div_source_status')    
        ]),    
    ],justify='start'),
    dbc.Row([
        dbc.Col([
        html.Div(id='div_metric_dataframe')   
            ])
    ],justify='start'),   #row  
    ],fluid=True) #container
    return section

def create_tab2_schema():
    section=dbc.Container([
    dbc.Row([
    document_layout(),
    ],justify='start'),   #row  
    # html.Br(),
    # html.Br(),
    dbc.Row([
    html.Div(id='div_doc_status')    
    ],justify='start') 
    ],fluid=True) #container
    return section

def create_tab3_schema():
    section=dbc.Container([
    dbc.Row([
    dbc.Col([    
    filter_layout(),
    html.Div(id='div_filter_status')  
    ]),
    dbc.Col([
    grouping_layout(),
    html.Div(id='div_grouping_status')  
    ]),
    ],justify='start'),   #row  
    dbc.Row([
    dbc.Col([
    html.Br(),
    # html.Button('REFRESH DATA',id='btn_refresh_data',style=css_button),   
    html.Div(id='div_filter_dataframe')     
        ]) 
    ],justify='start'),
    ],fluid=True) #container
    return section

def create_tab4_schema():
    section=dbc.Container([
    dbc.Row([
    dbc.Col([
    calculation_layout(),
    formatting_layout(),
    ]),
    dbc.Col([
    calculated_layout(),
    html.Div(id='div_calc_status')
    ]),
    ],justify='start'),   #row  
    html.Br(),
    # html.Br(),
    dbc.Row([
    ],justify='start') 
    ],fluid=True) #container
    return section

def create_tab5_schema():
    section=dbc.Container([
    dbc.Row([
    dbc.Col([    
    verbiage_layout(),
    html.Div(id='div_verbiage_status')
    ],width=4),
    dbc.Col([
    verbiage_data_layout(),    
        ],width=8)
    ],justify='start'),   #row  
    html.Br(),
    html.Br(),
    dbc.Row([
    # html.Div(id='div_grouping_status')      
    ],justify='start') 
    ],fluid=True) #container
    return section

def create_tab6_schema():
    section=dbc.Container([
    dbc.Row([
    values_layout(),
    ],justify='start'),   #row  
    html.Br(),
    html.Br(),
    dbc.Row([
    html.Div(id='div_values_status')  
    ],justify='start') 
    ],fluid=True) #container
    return section

def create_tab7_schema():
    section=dbc.Container([
    dbc.Row([
    data_sources_layout(),
    ],justify='start'),   #row  
    html.Br(),
    html.Br(),
    dbc.Row([
    ],justify='start') 
    ],fluid=True) #container
    return section



def create_tab8_schema():
    section=dbc.Container([
    dbc.Row([
    dbc.Col([    
    metric_layout(),
    html.Br(),html.Br(),
    source_layout(),
    html.Div(id='div_source_status'), 
    html.Br(),
    html.Button('VIEW DATA',id='btn_refresh_dataframe',style=css_button),  
    html.Button("SAVE METRIC",style=css_button,id='btn_save_metric'),
    html.Div(id='div_metric_status'), 
    ]),
    
    dbc.Col([
    filter_layout(),  
    html.Div(id='div_filter_status') ,
    grouping_layout(),
    html.Div(id='div_grouping_status'),    
    ])
    ],justify='start'),   #row  
    html.Br(),
    html.Br(),
    dbc.Row([
    dbc.Col([
    html.Div(id='div_filter_dataframe')    
    ])    
    ],justify='start') 
    ],fluid=True) #container
    return section

def create_tab9_schema():
    section=dbc.Container([
    dbc.Row([
        
    dbc.Col([
    data_sources_layout(),
    html.Div(id='div_values_status'),
    ]),
    
    
    dbc.Col([
    values_layout()
    ]),
    
    ],justify='start'),   #row  
    
    dbc.Row([
    
    
    # ]),
    dbc.Col([
    verbiage_output_layout()
    ])
    
    ])
    ],fluid=True) #container
    return section



loadData()
metrics=get_metrics()
# verbiage=get_saved_verbiage()
source_metadata=get_datasource_metadata()
calculation_metadata=get_metric_values_metadata()


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],suppress_callback_exceptions = True)
# app = dash.Dash(__name__, external_stylesheets ='https://codepen.io/chriddyp/pen/bWLwgP.css')
app.layout=dbc.Tabs([
    dbc.Tab(label='DOCUMENTS',tab_id='tab_Document',children=[create_tab2_schema()]),
    dbc.Tab(label='METRIC',tab_id='tab_Metric',children=[create_tab8_schema()]),
    dbc.Tab(label='CALCULATION',tab_id='tab_Calculation',children=[create_tab4_schema()]),
    dbc.Tab(label='VERBIAGE',tab_id='tab_Verbiage',children=[create_tab5_schema()]),
    dbc.Tab(label='METADATA',tab_id='tab_Metadata',children=[create_tab9_schema()]),

    ],id='all_tabs')   

################################################################################################################################
################################################################################################################################
###################################SAVE RECORD CALLBACKS########################################################################
################################################################################################################################
################################################################################################################################
@app.callback(
Output('div_doc_status','children'),
Input('btn_save_document','n_clicks'),
State('text_doc_nm','value'),
State('text_doc_desc','value'),       
prevent_initial_call=True)
def add_doc(n_clicks,docName,docDesc):
    sql_dict={
        'dataset' : 'NARRATIVE',
        'table' : 'DOCUMENT',
        'pk' : ['DOC_NM'],
        'DOC_NM' : [docName],
        'DOC_DESC' : [docDesc],
        }
    z=merge_sql(**sql_dict)    
    dt=datetime.datetime.now().strftime('%H:%M:%S')
    return f"{z} records were added to the METRIC_DOC table at {dt}"

@app.callback(
Output('div_metric_status','children'),
Input('btn_save_metric','n_clicks'),
State('text_metric_nm','value'),
State('text_metric_desc','value'),  
State('text_metric_doc','value'),
State('src_project_nm','value'),  
State('src_dataset_nm','value'), 
State('src_table_nm','value'),
State('filters_table','data'),
State('grouping_table','data'),
State('grouping_table','selected_rows'),
State('checkbox_copy_metric','value'),
State('select_metric_copy','value'),
prevent_initial_call=True)
def add_metric(n_clicks,metricName,metricDesc,docName,project,dataset,table,filtersData,groupingData,groupingRows,copyCheckbox,metricCopy):
    global currMetric,metrics
    currMetric=metricName
    recordCount=0
    tableCount=0
    sql_dict={
        'dataset' : 'NARRATIVE',
        'table' : 'METRIC',
        'pk' : ['METRIC_NM'],
        'METRIC_NM' : [metricName],
        'METRIC_DESC' : [metricDesc],
        'METRIC_DOC' : [docName],
        }
    z=merge_sql(**sql_dict)  
    recordCount+=z
    tableCount+=1
    
    sql_dict={
        'dataset' : 'NARRATIVE',
        'table' : 'METRIC_SRC',
        'pk' : ['METRIC_NM'],
        'METRIC_NM' : [metricName],
        'METRIC_PROJ' : [project],
        'METRIC_DSET' : [dataset],
        'METRIC_TBL' : [table]
        }
    z=merge_sql(**sql_dict)
    recordCount+=z
    tableCount+=1
        
    sql_dict={
            'dataset' : 'NARRATIVE',
            'table' : 'METRIC_FILTERS',
            'pk' : ['METRIC_NM'],
            # 'METRIC_NM' : [metricName for d in filtersData],
            # 'METRIC_COL' : [d['COLUMN'] for d in filtersData],
            # 'METRIC_OPERATOR' : [d['OPERATOR'] for d in filtersData],
            # 'METRIC_VAL' : [d['VALUE'] for d in filtersData]
    }
    
    if not filtersData is None:
        sql_dict['METRIC_NM']= [metricName for d in filtersData]
        sql_dict['METRIC_COL']= [d['COLUMN'] for d in filtersData]
        sql_dict['METRIC_OPERATOR']=[d['OPERATOR'] for d in filtersData]
        sql_dict['METRIC_VAL']=[d['VALUE'] for d in filtersData]
    else:
        sql_dict['METRIC_NM']= []
        sql_dict['METRIC_COL']= []
        sql_dict['METRIC_OPERATOR']=[]
        sql_dict['METRIC_VAL']=[]
        
    
    
    sql=f"DELETE FROM `NARRATIVE.METRIC_FILTERS` WHERE METRIC_NM='{metricName}'"
    z=dml_query(sql)
    z=insert_sql(**sql_dict)
    recordCount+=z
    tableCount+=1
    
    
    sql_dict={
            'dataset' : 'NARRATIVE',
            'table' : 'METRIC_GRP',
            'pk' : ['METRIC_NM']
             }
    
    if not groupingRows is None:
        sql_dict['METRIC_NM']=[metricName for r in groupingRows]
        sql_dict['METRIC_COL']=[groupingData[r]['COLUMN'] for r in groupingRows]
    else:
        sql_dict['METRIC_NM']=[]
        sql_dict['METRIC_COL']=[]
   
    sql=f"DELETE FROM `NARRATIVE.METRIC_GRP` WHERE METRIC_NM='{metricName}'"
    z=dml_query(sql)
    z=insert_sql(**sql_dict)
    recordCount+=z
    tableCount+=1
    if not copyCheckbox is None:
        if len(copyCheckbox)>0:
            cnt=update_copied_metric(metricCopy,metricName)
            recordCount+=cnt
            tableCount+=3
    # return f"{tableCount} tables were affected and {recordCount} records were updated"
    dt=datetime.datetime.now().strftime('%H:%M:%S')
    return f"{metricName} was added at {dt}"

# @app.callback(
# Output('div_source_status','children'),
# # Output('div_metric_dataframe','children'),
# Input('btn_save_source','n_clicks'),
# State('src_metric_nm','value'),
# State('src_project_nm','value'),  
# State('src_dataset_nm','value'), 
# State('src_table_nm','value'), 
# prevent_initial_call=True)
# def add_source(n_clicks,metricName,projectName,datasetName,tableName):
#     sql_dict={
#         'dataset' : 'NARRATIVE',
#         'table' : 'METRIC_SRC',
#         'pk' : ['METRIC_NM'],
#         'METRIC_NM' : [metricName],
#         'METRIC_PROJ' : [projectName],
#         'METRIC_DSET' : [datasetName],
#         'METRIC_TBL' : [tableName]
#         }
#     # return str(sql_dict)
#     z=merge_sql(**sql_dict) 
#     msg=f"{z} records were updated in the METRIC_SRC table..."
#     # d=generate_dataframe(tableName)
#     return msg

# @app.callback(
# Output('div_filter_status','children'),
# Input('btn_save_filter','n_clicks'),
# Input('btn_delete_filter','n_clicks'),
# State('filters_table','data'),
# State('filters_table','selected_rows'),
# State('filter_metric_nm','value'),
# prevent_initial_call=True)
# def add_filter(clicks1,click2,data,rows,metricName):
#     ctx=dash.callback_context
#     sql_dict={
#             'dataset' : 'NARRATIVE',
#             'table' : 'METRIC_FILTERS',
#             'pk' : ['METRIC_NM'],
#             'METRIC_NM' : [metricName for r in rows],
#             'METRIC_COL' : [data[r]['COLUMN'] for r in rows],
#             'METRIC_OPERATOR' : [data[r]['OPERATOR'] for r in rows],
#             'METRIC_VAL' : [data[r]['VALUE'] for r in rows]
#             }
#     if ctx.triggered[0]['prop_id'].startswith('btn_save_filter'):   
#         z=insert_sql(**sql_dict) 
#         msg=f"{z} records were inserted into the METRIC_FILTERS table..."
#         return msg
#     elif ctx.triggered[0]['prop_id'].startswith('btn_delete_filter'): 
#         z=delete_sql(**sql_dict) 
#         msg=f"{z} records were deleted from the METRIC_FILTERS table..."
#         return msg

# @app.callback(
# Output('div_grouping_status','children'),
# Input('btn_save_grouping','n_clicks'),
# State('grp_metric_nm','value'),
# State('grouping_table','data'),
# State('grouping_table','selected_rows'),
# prevent_initial_call=True)
# def add_grouping(clicks,metricName,data,rows):
#     ctx=dash.callback_context
#     sql_dict={
#             'dataset' : 'NARRATIVE',
#             'table' : 'METRIC_GRP',
#             'pk' : ['METRIC_NM'],
#             'METRIC_NM' : [metricName for r in rows],
#             'METRIC_COL' : [data[r]['COLUMN'] for r in rows],
#              }
#     if ctx.triggered[0]['prop_id'].startswith('btn_save_grouping'):   
#         msg=insert_sql(**sql_dict) 
#         return msg
    
@app.callback(
Output('div_calc_status','children'),
Input('btn_save_calculation','n_clicks'),
State('calc_metric_nm','value'),#metric name
State('calc_metric_type','value'), #metric type
State('calc_metric_column','value'),#metric column
State('metric_calculation','value'),#metric calculation
State('checkbox_topn','value'),
State('select_rank','value'),
State('text_order_by','value'),
State('select_ascending','value'),
State('select_format_type','value'),
State('text_rounding','value'),
State('select_units','value'),
State('checkbox_currency','value'),
prevent_initial_call=True)    
def save_metric_calculation(clicks,metricName,metricType,metricColumn,metricCalculation,topn,metricRank,metricOrderBy,metricAscending,formatType,rounding,units,currency):
    sql_dict={}
    sql_dict['dataset']='NARRATIVE'
    sql_dict['table']='METRIC_VAL'
    sql_dict['pk']=['METRIC_NM']
    sql_dict['METRIC_NM']=[metricName]
    if metricType=='CALCULATION':
        sql_dict['METRIC_COL']=[]
        sql_dict['METRIC_CALC']=[metricCalculation]
    elif metricType=='COLUMN':
        sql_dict['METRIC_COL']=[metricColumn]
        sql_dict['METRIC_CALC']=[]
    z=merge_sql(**sql_dict) 
    msg=f"{z} records were updated in the METRIC_VAL table..."
    
    if topn is None or len(topn)==0:
        pass
        
    else:
        calc_dict={}
        calc_dict.update({'dataset' : 'NARRATIVE'})
        calc_dict.update({'table' : 'METRIC_TOP_N'})
        calc_dict.update({'pk' : ['METRIC_NM']})
        calc_dict.update({'METRIC_NM' : [metricName]})
        calc_dict.update({'METRIC_RANK' : [metricRank]})
        calc_dict.update({'METRIC_ASCENDING' : [metricAscending]})
        calc_dict.update({'METRIC_ORDER_BY' : [metricOrderBy]})
        z=merge_sql(**calc_dict) 
        msg=msg[0:-3] + f" and {z} records in the METRIC_TOP_N table"

    format_dict={}
    format_dict.update({'dataset' : 'NARRATIVE'})
    format_dict.update({'table' : 'METRIC_FORMATTING'})
    format_dict.update({'pk' : ['METRIC_NM']})
    format_dict.update({'METRIC_NM' : [metricName]})
    format_dict['FORMAT_TYPE']=[formatType]
    format_dict['ROUNDING']=[rounding]
    if units is None:
        format_dict['UNITS']=['']
    else:
        format_dict['UNITS']=[units]
    if currency is None or len(currency)==0:
        format_dict['CURRENCY']=[""]
    else:
        format_dict['CURRENCY']=["$"]
    z=merge_sql(**format_dict)     
    dt=datetime.datetime.now().strftime('%H:%M:%S')
    return f"{metricName} was added at {dt}"
    # return z
        

@app.callback(
Output('div_datasource_status','children'),
Output('data_sources_table','data'),
Output('data_sources_table','selected_rows'),
Input('btn_refresh_datasources','n_clicks'),
State('data_sources_table','data'),
State('data_sources_table','selected_rows'),
prevent_initial_call=True)
def refresh_data_sources(clicks,data,rows):
    global source_metadata
    cnt=0
    if len(rows)==0:
        return None
    else:
        for r in rows:
            tableName=data[r]['DATA SOURCE'].split('.')[2]
            refresh_df_dictionary('NARRATIVE_V',tableName)
            sql_dict={}
            sql_dict['dataset']='NARRATIVE'
            sql_dict['table']='METADATA'
            sql_dict['pk']=['DATA_SRC']
            sql_dict['DATA_SRC']=[data[r]['DATA SOURCE']]
            sql_dict['LST_UPDT_TM']=['CURRENT_TIMESTAMP()']
            z=merge_sql(**sql_dict)
            cnt+=z
        msg=f"{cnt} records were updated in the Metadata table..."    
        source_metadata=get_datasource_metadata()    
        return msg,source_metadata,[]    
            
            
@app.callback(
Output('div_verbiage_status','children'),
Output('verbiage_saved_table','data'),
Input('btn_save_processed_verbiage','n_clicks'),
Input('btn_delete_verbiage','n_clicks'),
Input('btn_update_verbiage','n_clicks'),
Input('verbiage_doc','value'),
State('verbiage_paragraph','value'),
State('verbiage_sentence','value'),
State('text_raw_verbiage','value'),
State('verbiage_metrics_table','data'),
State('checkbox_verbiage_attributes','value'),
State('verbiage_saved_table','data'),
State('verbiage_saved_table','selected_rows'),
prevent_initial_call=True)   
def add_verbiage(clicks,clicks2,clicks3,doc,paragraph,sentence,rawVerbiage,data,attributes,data2,rows):
    global verbiage
    ctx=dash.callback_context
    if ctx.triggered[0]['prop_id'].startswith('btn_save_processed_verbiage'):
        sql_dict={}
        sql_dict['dataset']='NARRATIVE'
        sql_dict['table']='METRIC_VERBIAGE'
        sql_dict['pk']=['DOCUMENT_NM','PARAGRAPH_NBR','SENTENCE_NBR']    
        sql_dict['DOCUMENT_NM']=[doc]
        sql_dict['PARAGRAPH_NBR']=[paragraph]
        sql_dict['SENTENCE_NBR']=[sentence]
        sql_dict['VERBIAGE']=[rawVerbiage]
        sql_dict['PARAMETERS']=[d['METRIC'] for d in data]
        sql_dict['PARAMETERS']=["[" + ','.join([d['METRIC'] for d in data]) + "]"]
        if attributes is None:
            sql_dict['NEW_LINE']=['FALSE']
            sql_dict['ALIGNMENT']=['FALSE']
            sql_dict['BULLETS']=['FALSE']
        elif len(attributes)==0:
            sql_dict['NEW_LINE']=['FALSE']
            sql_dict['ALIGNMENT']=['FALSE']
            sql_dict['BULLETS']=['FALSE']
        else:
            if 'New Line' in attributes:
                sql_dict['NEW_LINE']=['TRUE']
            elif 'New Line' not in attributes:
                sql_dict['NEW_LINE']=['FALSE']
            if 'Justify' in attributes:
                sql_dict['ALIGNMENT']=['JUSTIFY']
            elif 'Justify' not in attributes:
                sql_dict['ALIGNMENT']=['NONE']
            if 'Bullets' in attributes:
                sql_dict['BULLETS']=['TRUE']
            elif 'Bullets' not in attributes:
                sql_dict['BULLETS']=['FALSE']
        z=merge_sql(**sql_dict)   
        verbiage=get_saved_verbiage(doc)
        if z > 0:
            dt=datetime.datetime.now().strftime('%H:%M:%S')
            return [f"The verbiage was added at {dt}",verbiage]
        else:
            return ["No records were added",verbiage]
    elif ctx.triggered[0]['prop_id'].startswith('btn_delete_verbiage'): 
        verbiage=get_saved_verbiage(doc)
        if data2 is None:
            return None,verbiage
        elif len(rows)==0:
            return None,verbiage
        else:
       
          sql_dict={
              'dataset' : 'NARRATIVE',
              'table' : 'METRIC_VERBIAGE',
              'pk' : ['DOCUMENT_NM','PARAGRAPH_NBR','SENTENCE'],
              'DOCUMENT_NM' : [doc for r in rows],
              'PARAGRAPH_NBR' : [data2[r]['PARAGRAPH'] for r in rows],
              'SENTENCE_NBR' : [data2[r]['SENTENCE'] for r in rows],
              }
          # return str(sql_dict),verbiage
          z=delete_sql(**sql_dict)
          dt=datetime.datetime.now().strftime('%H:%M:%S')
          verbiage=get_saved_verbiage(doc)
          return z,verbiage
        
    elif ctx.triggered[0]['prop_id'].startswith('verbiage_doc'):   
        verbiage=get_saved_verbiage(doc)
        return None,verbiage
    
    elif ctx.triggered[0]['prop_id'].startswith('btn_update_verbiage'):
        sql_dict={}
        sql_dict['dataset']='NARRATIVE'
        sql_dict['table']='METRIC_VERBIAGE'
        sql_dict['pk']=['DOCUMENT_NM','PARAGRAPH_NBR','SENTENCE_NBR']    
        sql_dict['DOCUMENT_NM']=[doc]
        sql_dict['PARAGRAPH_NBR']=[data2[rows[0]]['PARAGRAPH']]
        sql_dict['SENTENCE_NBR']=[data2[rows[0]]['SENTENCE']]
        sql_dict['VERBIAGE']=[data2[rows[0]]['VERBIAGE']]
        sql_dict['PARAMETERS']=[data2[rows[0]]['PARAMETERS']]
        sql_dict['NEW_LINE']=[data2[rows[0]]['NEW LINE']]
        sql_dict['ALIGNMENT']=[data2[rows[0]]['JUSTIFY']]
        sql_dict['BULLETS']=[data2[rows[0]]['BULLETS']]
        # return str(sql_dict),data2
        z=merge_sql(**sql_dict)   
        verbiage=get_saved_verbiage(doc)
        paragraph=[data2[r]['PARAGRAPH'] for r in rows][0]
        sentence=[data2[r]['SENTENCE'] for r in rows][0]
        x=assign_verbiage_output(doc,paragraph,sentence)
        
        if z > 0:
            dt=datetime.datetime.now().strftime('%H:%M:%S')
            return f"The verbiage was refreshed at {dt}",verbiage
        else:
            return "No records were refreshed",verbiage
        
    
    
    
    else:
        verbiage=get_saved_verbiage(doc)
        return 'Again',verbiage
    
            
            
#################################################################################################################################
#################################################################################################################################
##################################################CASCADING PARAMETER CALLBACKS##################################################
#################################################################################################################################
#################################################################################################################################
# @app.callback(
# Output('filter_column_nm','options'),
# Input('filter_metric_nm','value'),
# prevent_initial_call=True)
# def update_filter_columns(metricName):
#     try:
#         return get_grouping_column_filters(metricName)
#     except:
#         return None

@app.callback(
Output('filter_val_nm','options'),
Input('filter_column_nm','value'),
State('text_metric_nm','value'),
prevent_initial_call=True)
def update_filter_values(columnName,metricName):
    global metadata_dict,dataframe_dict
    table=dataframe_dict['METRIC_SRC']
    table=table[table['METRIC_NM']==metricName]
    table=table['METRIC_TBL'].values[0]
    df=dataframe_dict[table]
    return [{'label' : n, 'value' : n} for n in list(set(df[columnName].tolist()))]

# @app.callback(
# Output('src_metric_nm','value'),
# Input('text_metric_nm','value'),
# prevent_initial_call=True)
# def update_source_metric_name(metricName):
#     return metricName

# @app.callback(
# Output('src_project_nm','value'),
# Output('src_dataset_nm','value'),
# Output('src_table_nm','value'),
# Input('src_metric_nm','value'),
# prevent_initial_call=True)
# def update_source_values(metricName):
#     global dataframe_dict
#     try:
#         df=dataframe_dict['METRIC_SRC']
#         df=df[df['METRIC_NM']==metricName]
#         return df['METRIC_PROJ'].values[0],df['METRIC_DSET'].values[0],df['METRIC_TBL'].values[0]
#     except:
#         return None,None,None

@app.callback(
Output('calc_metric_column','options'),
Output('available_columns_table','data'),
Input('calc_metric_nm','value'),
prevent_initial_call=True)
def update_calc_columns(metricName):
    return get_all_columns(metricName),get_metric_columns(metricName)




@app.callback(
Output('select_metric_copy','disabled'),
Input('checkbox_copy_metric','value'),
State('select_metric_copy','disabled'),
prevent_initial_call=True)
def update_copy_metric(clicks,disabled):
    return not disabled



##################################################################################################################################
##################################################################################################################################
#######################################TABLE CALLBACKS############################################################################
##################################################################################################################################
##################################################################################################################################
# @app.callback(
# Output('filters_table','data'),
# # Output('filters_table','selected_rows'),
# Input('btn_add_filter','n_clicks'),
# Input('filter_metric_nm','value'),
# State('filter_column_nm','value'),
# State('filter_operator','value'),
# State('filter_val_nm','value'),
# State('filters_table','data'),
# prevent_initial_call=True)
# def update_filters_table(clicks,metricName,column,operator,value,data):
#         global dataframe_dict
#         ctx=dash.callback_context
#         if ctx.triggered[0]['prop_id'].startswith('btn_add_filter'):
#             df=pd.DataFrame(columns=['COLUMN','OPERATOR','VALUE'],data=[[column,operator,value]])
#             if data is None:
#                 return df.to_dict(orient='records')
#             else:
#                 data.append(df.to_dict('records')[0])
#                 return data
#         elif ctx.triggered[0]['prop_id'].startswith('filter_metric_nm'):
#                 df=dataframe_dict['METRIC_FILTERS'].copy()
#                 df=df[df['METRIC_NM']==metricName]
#                 if not df.empty:
#                     df.drop(axis=1,columns=['METRIC_NM'],inplace=True)
#                     df.rename(columns={'METRIC_COL' : 'COLUMN', 'METRIC_OPERATOR' : 'OPERATOR', 'METRIC_VAL' : 'VALUE'},inplace=True)
#                     return df.to_dict(orient='records')
#                 else:
#                     return None

# @app.callback(
# Output('grouping_table','data'),    
# Output('grouping_table','selected_rows'),
# Input('grp_metric_nm','value'),  
# State('grouping_table','data'),  
# prevent_initial_call=True)        
# def update_grouping_table(metricName,data):
#     global dataframe_dict
#     try:
#         df=dataframe_dict['METRIC_GRP']
#         df=df[df['METRIC_NM']==metricName]
#         groups=list(df['METRIC_COL'].tolist())
#         col_list=[]
#         data=get_grouping_columns(metricName)
#         for index,d in enumerate(data):
#             if d['COLUMN'] in groups:
#                 col_list.append(index)
#         return data,col_list
#     except:
#         return None,None
    
@app.callback(
Output('verbiage_metrics_table','data'),
Input('btn_add_verbiage_metric','n_clicks'),
Input('btn_delete_verbiage_metric','n_clicks'),
State('verbiage_metric_name','value'),
State('verbiage_metrics_table','data'),
State('verbiage_metrics_table','selected_rows'),
prevent_initial_call=True)  
def update_verbiage_metrics(clicks,clicks2,metricName,data,rows):
    ctx=dash.callback_context
    if ctx.triggered[0]['prop_id'].startswith('btn_add_verbiage_metric'):
        if not data is None:
            data.append({'METRIC' : metricName})
        else:
            data=[{'METRIC' : metricName}]
        return data 
    elif ctx.triggered[0]['prop_id'].startswith('btn_delete_verbiage_metric'):
        if len(rows)>0:
            for r in rows:
                del data[r]
        return data        
        
###############################################################################################################################
###############################################################################################################################
##################################################OTHER CALLBACKS##############################################################
###############################################################################################################################
###############################################################################################################################
@app.callback(
Output('text_metric_desc','value'),
Output('text_metric_doc','value'),
Output('src_project_nm','value'),
Output('src_dataset_nm','value'),
Output('src_table_nm','value'),
Output('filter_column_nm','options'),
Output('filters_table','data'),
Output('grouping_table','data'),
Output('grouping_table','selected_rows'),
Input('text_metric_nm','value'),
Input('select_metric_copy','value'),
State('checkbox_copy_metric','value'),
State('select_metric_copy','value'),
State('text_metric_desc','value'),
State('text_metric_doc','value'),
State('src_project_nm','value'),
State('src_dataset_nm','value'),
State('src_table_nm','value'),
State('filter_column_nm','options'),
State('filters_table','data'),
State('grouping_table','data'),
State('grouping_table','selected_rows'),
prevent_initial_call=True)
def get_metrics_metadata(metricName,copyMetricName,checkboxCopy,copyMetric,metricDesc,metricDoc,metricProject,metricDataset,metricTable,filterColumn,filterData,groupingData,groupingRows):
    df=pd.read_gbq("SELECT * FROM `NARRATIVE.VW_METRICS`",project_id='analytics-da-reporting-thd',dialect='standard')
    ctx=dash.callback_context
    #if user selected an existing metric
    if ctx.triggered[0]['prop_id'].startswith('text_metric_nm'):
        if not copyMetric is None and len(checkboxCopy)>0:
            return metricDesc,metricDoc,metricProject,metricDataset,metricTable,filterColumn,filterData,groupingData,groupingRows
    #if user wants to copy a metric        
    elif ctx.triggered[0]['prop_id'].startswith('select_metric_copy'):
        metricName=copyMetricName   
    df=df[df['METRIC_NAME']==metricName]    
    if df.empty:
        return None,None,None,None,None,None,None,None,None
    else:
        metricDesc=df['METRIC_DESCRIPTION'].values[0]
        metricDoc=df['METRIC_DOC'].values[0]
        metricProject=df['PROJECT'].values[0]
        metricDataset=df['DATASET'].values[0]
        metricTable=df['VIEW'].values[0]
        metricFilterColumns=get_grouping_column_filters(metricName)
        df=dataframe_dict['METRIC_FILTERS'].copy()
        df=df[df['METRIC_NM']==metricName]
        if not df.empty:
            df.drop(axis=1,columns=['METRIC_NM'],inplace=True)
            df.rename(columns={'METRIC_COL' : 'COLUMN', 'METRIC_OPERATOR' : 'OPERATOR', 'METRIC_VAL' : 'VALUE'},inplace=True)
            metricFilterData=df.to_dict(orient='records')
        else:
            metricFilterData=None
        df=dataframe_dict['METRIC_GRP']
        df=df[df['METRIC_NM']==metricName]
        groups=list(df['METRIC_COL'].tolist())
        col_list=[]
        data=get_grouping_columns(metricName)
        for index,d in enumerate(data):
            if d['COLUMN'] in groups:
                col_list.append(index)
        metricGroupingData=data
        metricGroupingRows=col_list        
        return metricDesc,metricDoc,metricProject,metricDataset,metricTable,metricFilterColumns,metricFilterData,metricGroupingData,metricGroupingRows



@app.callback(
Output('calc_metric_type','value'),
Output('calc_metric_column','value'),
Output('metric_calculation','value'),
Output('checkbox_topn','value'),
Output('select_rank','value'),
Output('text_order_by','value'),
Output('select_ascending','value'),
Output('select_format_type','value'),
Output('text_rounding','value'),
Output('select_units','value'),
Output('checkbox_currency','value'),
Input('calc_metric_nm','value'),
prevent_initial_call=True)
def get_calculation_metadata(metricName):
    df=pd.read_gbq("SELECT * FROM `NARRATIVE.VW_METRICS`",project_id='analytics-da-reporting-thd',dialect='standard')
    df=df[df['METRIC_NAME']==metricName]
    if df.empty:
        return None,None,None,None,None,None,None,None,None,None,None
    else:
        if df['METRIC_COL'].isnull().values.all():
        # if pd.isnull(df['METRIC_COL']):
            metricType='CALCULATION'
            metricColumn=None
            metricCalculation=df['METRIC_CALC'].values[0]
        elif df['METRIC_CALC'].isnull().values.all():
            # pd.isnull(df['METRIC_CALC']):
            metricType='COLUMN'
            metricCalculation=None
            metricColumn=df['METRIC_COL'].values[0]
        # if pd.isnull(df['METRIC_RANK']):
        if df['METRIC_RANK'].isnull().values.all():    
            metricTopN=[]
            metricRank=None
            metricAscending=None
            metricOrderBy=None
        else:
            metricTopN=['Apply Top N?']
            metricRank=df['METRIC_RANK'].values[0]
            metricAscending=df['METRIC_ASCENDING'].values[0]
            metricOrderBy=df['METRIC_ORDER_BY'].values[0]
        
        metricFormatType=df['FORMAT_TYPE'].values[0]   
        metricRounding=df['ROUNDING'].values[0]
        metricUnits=df['UNITS'].values[0]
        if df['CURRENCY'].isnull().values.all() or len(df['CURRENCY'].values[0])==0:
            metricCurrency=[]
        else:
            metricCurrency=['Apply Currency?']
        return metricType,metricColumn,metricCalculation,metricTopN,metricRank,metricOrderBy,metricAscending,metricFormatType,metricRounding,metricUnits,metricCurrency    




















# @app.callback(
# Output('metric_calculation','value'),
# Output('text_order_by','value'),
# Input('btn_add_column','n_clicks'),
# Input('btn_add_orderby','n_clicks'),
# State('metric_calculation','value'),
# State('text_order_by','value'),
# State('available_columns_table','data'),
# State('available_columns_table','selected_rows'),    
# prevent_initial_call=True)
# def update_calc(clicks,clicks2,val,val2,data,rows):
#     ctx=dash.callback_context
#     if ctx.triggered[0]['prop_id'].startswith('btn_add_column'):
#         if len(rows)==0:
#             return val,val2
#         else:
#             if val is None:
#                 return  '[' + data[rows[0]]['COLUMN'] + ']',val2
#             else:
#                 return val +  '[' + data[rows[0]]['COLUMN'] + ']',val2
#     elif ctx.triggered[0]['prop_id'].startswith('btn_add_orderby'): 
#         if len(rows)==0:
#             return val,val2
#         else:
#             if val2 is None:
#                 return  val,'[' + data[rows[0]]['COLUMN'] + ']'
#             else:
#                 return val,val2 +  '[' + data[rows[0]]['COLUMN'] + ']'
        
        
            
            
            
            
        
@app.callback(
Output('text_metric_value','value'),
Output('text_metric_formatted','value'),
Input('btn_calculate_metric','n_clicks'),
State('calc_metric_nm','value'),
State('calc_metric_type','value'),
State('calc_metric_column','value'),
State('metric_calculation','value'),
State('checkbox_topn','value'),
State('select_rank','value'),
State('text_order_by','value'),
State('select_ascending','value'),
State('select_format_type','value'),
State('text_rounding','value'),
State('select_units','value'),
State('checkbox_currency','value')
,prevent_initial_call=True)  
def calculation_metric(clicks,metricName,metricType,column,calc,topn,rank,orderby,ascending,formatType,rounding,units,currency):
    calc_dict={}
    format_dict={}
    calc_dict.update({'metricName' : metricName})   
    if metricType=='COLUMN':
        calc_dict.update({'column' : column})
        calc_dict.update({'calculation' : None})
    elif metricType=='CALCULATION':
        calc_dict.update({'column' : None})
        calc_dict.update({'calculation' : calc})
    
    if topn is None or len(topn)==0:
        pass
        
    else:
        calc_dict.update({'RANK' : rank})
        calc_dict.update({'ORDERBY' : orderby})
        calc_dict.update({'ASCENDING' : ascending})
    
    format_dict.update({'TYPE' : formatType})
    format_dict.update({'ROUNDING' : rounding})
    format_dict.update({'UNITS' : units})
    
    if currency is None:
        pass
    elif len(currency)==0:
        pass
    else:
        format_dict.update({'CURRENCY' : currency})

    
    # try:
    if 1==1:    
        calc=calculate_value(**calc_dict) 
        formatted_calc=assign_formatted_values(calc,**format_dict)
        return calc,formatted_calc
    # except Exception as e:
    #     return str(e)
        
@app.callback(
Output('div_filter_dataframe','children'),
Input('btn_refresh_dataframe','n_clicks'),
State('text_metric_nm','value'),
State('filters_table','data'),
State('filters_table','selected_rows'),
State('grouping_table','data'),
State('grouping_table','selected_rows'),  
prevent_initial_call=True)
def update_filtered_dataframe(clicks,metricName,filters_data,filters_rows,groupby_data,groupby_rows):
    filters_dict={}
    rows_dict={}
    groupby=[]
    df=dataframe_dict['METRIC_SRC'].copy()
    df=df[df['METRIC_NM']==metricName]
    table=df['METRIC_TBL'].values[0]   
    filters_dict['table']=table   
    
    # if not filters_rows is None:
    #     for r in filters_rows:
    #           rows_dict.update({filters_data[r]['COLUMN'] : filters_data[r]['VALUE']})
    #     filters_dict['filters']=rows_dict    
    # else:
    #     filters_dict['filters']=None
    if not filters_data is None:
        for d in filters_data:
            rows_dict.update({d['COLUMN'] : d['VALUE']})
        filters_dict['filters']=rows_dict
    else:
        filters_dict['filters']=None
    
    
    
    
    
    
    
    if len(groupby_rows)>0:
        for r in groupby_rows:
              groupby.append(groupby_data[r]['COLUMN'])
        filters_dict['groupby']=groupby 
    else:
         filters_dict['groupby']=None
    # return str(filters_dict)     
    return generate_filtered_dataframe(**filters_dict)

@app.callback(
Output('div_values_status','children'),
Output('metric_values_table','data'),
Output('metric_values_table','selected_rows'),
Input('btn_refresh_metric_values','n_clicks'),
Input('btn_refresh_metric_values_all','n_clicks'),
State('metric_values_table','data'),
State('metric_values_table','selected_rows'),
prevent_initial_call=True)    
def get_all_metric_values(clicks,clicks2,data,rows):
    cnt=0
    ctx=dash.callback_context
    if ctx.triggered[0]['prop_id'].startswith('btn_refresh_metric_values_all'):
        for d in data:
            metricName=d['METRIC']
            z=calculate_all_metrics(metricName)
            cnt+=z
        
        
    elif ctx.triggered[0]['prop_id'].startswith('btn_refresh_metric_values'): 
        if len(rows)==0:
            return None
        for r in rows:
            metricName=data[r]['METRIC']
            z=calculate_all_metrics(metricName)
            cnt+=z
        
    msg=f"{cnt} records were updated in the METRIC_VAL table..."
    calculation_metadata=get_metric_values_metadata()
    return msg,calculation_metadata,[]
    
@app.callback(        
Output('text_processed_verbiage','value'),    
Input('btn_create_processed_verbiage','n_clicks'),
State('metric_values_table','data'),
State('verbiage_metrics_table','data'),
State('text_raw_verbiage','value'),
prevent_initial_call=True)
def render_verbiage(clicks,data,data2,verbiage):
    values_dict={}
    for d in data:
        values_dict[d['METRIC']]=d['VALUE']
       
    metric_list=[]
    for d in data2:
        metric_list.append(d['METRIC'])
    
    for m in metric_list:
        verbiage=verbiage.replace('?',values_dict[m],1)
        
    return verbiage 

@app.callback(
Output('verbiage_output_table','data'),
Input('select_metadata_doc','value'),
prevent_initial_call=True)
def update_verbiage_output(docName):
	return get_verbiage_output(docName)    
    
#################################################################################################################################
#################################################################################################################################
####################################################TAB CALLBACKS################################################################
#################################################################################################################################
#################################################################################################################################
# @app.callback(
# Output('src_metric_nm','value'),    
# Input('all_tabs','active_tab'),
# prevent_initial_call=True
# )
# def update_source_tab(tab):
#     if tab=='SOURCE':
#         return currMetric
########################RUN APP#########################################
if __name__=='__main__':
    Timer(1, open_browser).start()
    app.run_server(debug=True)#,dev_tools_ui=False,dev_tools_props_check=False)#,mode='inline')