if __name__ =="__main__":
    import pandas as pd
    codes = pd.read_csv('codes.txt', sep=' ')
    vars_used = pd.read_csv('Variables_used.csv', index_col=0)
    print(vars_used)
    
    datatable = codes[codes['Code'].isin(vars_used['0'])]
    print(len(datatable))
    datatable.to_csv('vartable.csv')
    datatable = datatable.set_index('Code')
    print(datatable[['Description', 'TCode']].to_latex())