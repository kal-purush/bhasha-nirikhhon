import tools.utils as utils
import tools.constant as constant
import os
import pandas as pd

def to_Product_count(data):

    data = data.merge(data.groupby('Customer_ID').size().reset_index())
    data.rename(columns={0: 'count'}, inplace=True)

    data = data.drop(['Product'], axis=1).drop_duplicates(['Customer_ID'])
    # del_pro = del_pro.drop_duplicates(['Customer_ID'])
    # count['Customer_ID'] =count['Customer_ID'].astype(object)

    # 统计Product_Count
    data_count = data.drop(['Customer_ID', 'Total_Cost'], axis=1)
    pre_merge = data_count.merge(data.groupby('ValueBand').size().reset_index())
    pre_merge = pre_merge.drop_duplicates(['ValueBand']).drop(['count'], axis=1)
    s = data_count.groupby('ValueBand')['count'].mean().reset_index()
    data_count = pre_merge.merge(data_count.groupby('ValueBand')['count'].mean().reset_index(), on='ValueBand')
    data_count.rename(columns={'count': 'RECORD_COUNT_mean', 0: 'RECOURD_COUNT'}, inplace=True)

    # 导出表
    data_count.to_csv(os.path.join(constant.data_files_path, 'Product_count.dat'), sep=',', index=0)


def to_Value_band(data):
    Value_band = data.groupby(['Product', 'ValueBand']).size().reset_index()
    Value_band.rename(columns={0:'RECORD_COUNT'}, inplace=True)
    Value_band.to_csv(os.path.join(constant.data_files_path,'value_band&product.dat'),sep=',',index=0)

if __name__ == "__main__":

    f1 = open(os.path.join(constant.data_files_path, constant.cust_call_prod_name), encoding='utf8')
    data1 = pd.read_table(f1,",")
    f2 = open(os.path.join(constant.data_files_path, constant.products_dat_name), encoding='utf8')
    data2 = pd.read_table(f2, ",")
    right = pd.merge(data1, data2, how='right', left_on='Customer_ID', right_on='Customer_ID')
    data = right[['Customer_ID', 'Total_Cost', 'ValueBand', 'Product']]
    to_Product_count(data)
    to_Value_band(data)
    print('end')

