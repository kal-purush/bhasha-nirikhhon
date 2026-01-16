import pandas as pd

#############################
#############################
def load_data():
    dmnd = pd.read_csv("demand.csv")
    wthr = pd.read_csv("oa_temp.csv")

    return dmnd, wthr

#############################
#############################
def basic_clean(df_demand, df_weather):
    df_demand = df_demand.dropna(axis=0, how='any')
    df_weather = df_weather.dropna(axis=0, how='any')

    lis = df_demand.index[df_demand["Value"] <= 0.1].tolist()
    df_demand = df_demand.drop(lis)

    lis = df_weather.index[df_weather["Value"] >= 200].tolist()
    df_weather.drop(lis, inplace = True)
    
    print(max(df_weather["Value"]))

    return df_demand, df_weather


#############################
#############################
def match_index(df_demand, df_weather):
    strt = df_weather["Date"][0]
    end = df_demand["Date"][df_demand.shape[0]-1]

    idx_w = df_weather.index[df_weather["Date"] ==  end][0]
    idx_d = df_demand.index[df_demand["Date"] == strt][0]

    df_demand = df_demand[idx_d:df_demand.shape[0]-1]
    df_weather = df_weather[0:idx_w]
    df_demand = df_demand.reset_index()
    df_weather = df_weather.reset_index()
    
    df_demand = df_demand.drop(columns = ['index'])
    df_weather = df_weather.drop(columns = ['index'])

    return df_demand, df_weather
#############################
#############################
def uncommon(df_demand, df_weather):
    date_d = list(df_demand["Date"])
    date_w = list(df_weather["Date"])
    
    uncmn = df_weather["Date"].isin(date_d)
    uncmn_idx = uncmn.index[uncmn == False].tolist()
    
    df_weather.drop(uncmn_idx, inplace=True)
     
    return df_demand, df_weather


#############################
#############################
def main():
    df_demand, df_weather = load_data()
    df_1, df_2 = basic_clean(df_demand, df_weather)
    df_demand, df_weather = match_index(df_1, df_2)
    df_demand, df_weather = uncommon(df_demand, df_weather)
    return df_demand, df_weather



#############################
#############################
if __name__ == "__main__":
#    main()
    dmnd, wthr = main()
    dmnd.to_csv("clean_demand.csv", sep=',')
    wthr.to_csv("clean_weather.csv", sep=',')