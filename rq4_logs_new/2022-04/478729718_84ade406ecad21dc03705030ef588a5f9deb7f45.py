import pandas as pd
import re

def drop_columns_with_function(data):
    data2 = data.drop(columns=['Edition Statement', 'Corporate Author', 'Corporate Contributors', 'Former owner', 'Engraver', 'Issuance type', 'Shelfmarks'], axis=1)
    for col in data2.columns:
        print(col)

def drop_columns_with_read(filename):
    cols = list(pd.read_csv(filename, nrows=1))
    
    data = pd.read_csv(filename, usecols =[i for i in cols if i != 'Edition Statement' and i != 'Corporate Contributors' and i != 'Corporate Author' and i != 'Former owner' and i != 'Engraver' and i != 'Issuance type' and i != 'Shelfmarks'])
    for col in data.columns:
        print(col)

def tidy_data(data):
    dates = data['Date of Publication'].values.tolist()
    tidy_dates = []
    for date in dates:
        date = str(date)
        tidy_date = re.search('\d+|$', date).group()
        tidy_dates.append(tidy_date)
    
    print('\n\n\n')
    for date in tidy_dates:
        print(date)

def tidy_state(item):
    if ' (' in item:
        return item[:item.find(' (')]
    elif '[' in item:
        return item[:item.find('[')]
    else:
        return item
    
    
def tidy_uni(df):
    if '(' in df:
        return df[:df.find('(')]
    elif ')' in df:
        return df[:df.find(')')]
    else:
        return df
    
   
def uni_exercise(filename):
    lines = []
    f = open('unis.txt', 'r')
    for line in f:
        if '[edit]' in line:
            state = line.split('\n')[0]
            #state = state.applymap(tidy_state)
        else:
            city = line.split(' ',1)[0]
            city = city.split('\n')[0]
            university = line.split(' ',1)[1]
            university = university.split('\n')[0]
            university = university.replace('(','').replace(')','')
            each_uni = []
            if ',' in university:
                each_uni = university.split(',')
                for each in each_uni:
                    lines.append((state, city, each))
            else:
                lines.append((state, city, university))
    for line in lines:
        print(line)
    df = pd.DataFrame(lines, columns=['State', 'City', 'University'])
    print(df)
    df = df.applymap(tidy_state)
    print(df)
    df2 = df.copy()
    df2.dropna(inplace=True)
    print(df2)
    '''
    list = df2['University'].values.tolist()
    states = df2['State'].values.tolist()
    cities = df2['City'].values.tolist()
    lenState = len(states)
    lenCities = len(cities)
    lenUnis = len(list)
    print(f'Num States: {lenState}\nNum Cities: {lenCities}\nNum Unis: {lenUnis}\n')
    for i in range(lenState):
        if str(states[i]) == '' or str(states[i]) == 'nan':
            continue 
        else:
            print(f'{states[i]}, {cities[i]}, {list[i]}')
    '''   

if __name__ == "__main__":
    filename = 'books.csv'
    file2 = 'unis.txt'
    
    data = pd.read_csv(filename)
    
    for col in data.columns:
        print(col)
    print("\n\n\n")   
    drop_columns_with_function(data)
    print("\n\n\n")   
    drop_columns_with_read(filename)
    print("\n\n\n")   
    tidy_data(data)
    uni_exercise(file2) 
     