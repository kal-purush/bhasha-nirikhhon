from airflow.models import Variable
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

def __get_df_by_csv(dir):
    return pd.read_csv(dir, header=0, quotechar="'") 

def __data_cleaning(df):
    __age_str_to_int(df)
    __gender_only_M_F(df)
    __amount_zero_to_null(df)
    __create_date_columns(df)  

def __age_str_to_int(df):
    df["age"].mask(df["age"] == 'U', 0, inplace=True)
    df["age"] = df["age"].astype('int')

def __gender_only_M_F(df):
    df["gender"].where((df["gender"] == "M") | (df["gender"] == "F"), None, inplace=True)

def __amount_zero_to_null(df):
    df["amount"].mask(df["amount"] == 0, None, inplace=True)

def __create_date_columns(df):
    __year = date.today().year
    df["ano"] = __year

    df["mes"] = 0  
    df["mes"] = np.where((df["step"] >= 0) & (df["step"] <= 30), 1, df["mes"])
    df["mes"] = np.where((df["step"] >= 31) & (df["step"] <= 60), 2, df["mes"])
    df["mes"] = np.where((df["step"] >= 61) & (df["step"] <= 90), 3, df["mes"])
    df["mes"] = np.where((df["step"] >= 91) & (df["step"] <= 120), 4, df["mes"])
    df["mes"] = np.where((df["step"] >= 121) & (df["step"] <= 150), 5, df["mes"])
    df["mes"] = np.where((df["step"] >= 151) & (df["step"] <= 180), 6, df["mes"])  

    __day = 1
    df["data"] = __day

    df["data_aux"] = None
    df["data_aux"] = np.where((df["mes"] == 1), date(__year,1,__day), df["data_aux"])
    df["data_aux"] = np.where((df["mes"] == 2), date(__year,2,__day), df["data_aux"])
    df["data_aux"] = np.where((df["mes"] == 3), date(__year,3,__day), df["data_aux"])
    df["data_aux"] = np.where((df["mes"] == 4), date(__year,4,__day), df["data_aux"])
    df["data_aux"] = np.where((df["mes"] == 5), date(__year,5,__day), df["data_aux"])
    df["data_aux"] = np.where((df["mes"] == 6), date(__year,6,__day), df["data_aux"])  

def __get_df_sum(df):
    #cria dataframe agrupados
    __fact_merchant_kpi = df.groupby(["data_aux", "merchant"], as_index=False).agg(tpv=('amount','sum'), qtd_transacoes=('merchant','count'))
    __fact_customer_kpi = df.groupby(["data_aux", "customer"], as_index=False).agg(tpv=('amount','sum'), qtd_transacoes=('customer','count'))
    __dim_merchant_category = df.groupby(["merchant", "category"], as_index=False).any()[["merchant", "category"]]
    #renomeia colunas 
    __fact_merchant_kpi.rename(columns={"merchant":"id_merchant", "data_aux": "data"}, inplace=True)
    __fact_customer_kpi.rename(columns={"customer":"id_customer", "data_aux": "data"}, inplace=True)
    __dim_merchant_category.rename(columns={"merchant":"id_merchant"}, inplace=True)
    #deleta coluna data_aux
    df.drop(labels=["data_aux"], inplace=True, axis=1)
    #cria dict de criação das tabelas
    __transaction = {"df": df, "table": "transactions", "schema": "db", "index": True, "index_label": "id"}
    __fmk = {"df": __fact_merchant_kpi, "table": "fact_merchant_kpi", "schema": "analytic", "index": False, "index_label": ""}
    __fck = {"df": __fact_customer_kpi, "table": "fact_customer_kpi", "schema": "analytic", "index": False, "index_label": ""}
    __dmc = {"df": __dim_merchant_category, "table": "dim_merchant_category", "schema": "analytic", "index": False, "index_label": ""}
    
    return [__transaction, __fmk, __fck, __dmc] 

def __get_conn_mysql():
    __user_mysql = Variable.get("MYSQL_USER")
    __password_mysql = Variable.get("MYSQL_PASSWORD")
    __host_mysql = Variable.get("MYSQL_HOST")
    __port_mysql = Variable.get("MYSQL_PORT")
    __mysql_conn = f"mysql://{__user_mysql}:{__password_mysql}@{__host_mysql}:{__port_mysql}"
    return create_engine(__mysql_conn, echo=False)  

def __record_data(engine, df_dict_list):  
    for df_dict in df_dict_list:
        df_dict["df"].to_sql(
            con=engine,
            schema=df_dict["schema"],
            name=df_dict["table"],
            if_exists="replace",
            index=df_dict["index"],
            index_label=df_dict["index_label"]
        )

def transform():
    __transactions_csv = __get_df_by_csv("./tmp/banksim1/bs140513_032310.csv")
    __data_cleaning(__transactions_csv)
    __df_dict_list = __get_df_sum(__transactions_csv)
    __engine = __get_conn_mysql()
    __record_data(__engine, __df_dict_list)