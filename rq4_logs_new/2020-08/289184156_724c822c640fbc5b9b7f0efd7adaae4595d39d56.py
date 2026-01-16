from IPython.display import display, HTML
import pandas as pd
def show_info(data):
    tab_info=pd.DataFrame(data.dtypes).T.rename(index={0:'column type'})
    tab_info=tab_info.append(pd.DataFrame(data.isnull().sum()).T.rename(index={0:'null values (nb)'}))
    tab_info=tab_info.append(pd.DataFrame(data.isnull().sum()/data.shape[0]*100).T.rename(index={0:'null values(%)'}))
    return(display(tab_info))