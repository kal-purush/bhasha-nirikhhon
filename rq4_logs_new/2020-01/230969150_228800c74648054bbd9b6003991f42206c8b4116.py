
# coding: utf-8

# In[57]:


# Imports

import MySQLdb
import csv
import pandas as pd


# Establish connection to database

db = MySQLdb.connect(user = 'tier1marketspace',
                    password = 'poopypants',
                    host = 'tier1marketspace.mysql.pythonanywhere-services.com',
                    database = 'tier1marketspace$Tier1',
                    autocommit = True,
                    local_infile = 1)
print("Connection to DB Established")

# Section to create updated list of CardIDs in database to be used for individual .csv generator


#  Cursor

cursor = db.cursor() 

# Create list of all CardIDs in database

query = "SELECT CardID FROM Prices"

cursor.execute(query)

CardID_query = cursor.fetchall()

# Tuple to list

CardID_queryNew = [list(row) for row in CardID_query ]

# df for CardID

dfx = pd.DataFrame(CardID_queryNew, columns = ['CardID'])

# Convery df to .csv for "with open" to use to create list used in for loop

dfx.to_csv('CardID.csv', index=False)

with open('CardID.csv', 'rt') as f:
    
    reader = csv.reader(f)
    
    CardIDs_list = list(reader)
    
# For loop to convert "list of lists" to list
    
for sublist in CardIDs_list:
    
    for item in sublist:
        
        CardIDs_flat.append(item)
        
        
# _________Individual .csv file generator___________
        
        

# For loop to go over CardIDs list for database query

for card in CardIDs_flat:
    
    cursor.execute("SELECT * FROM Prices WHERE CardID IN ('%s')" % (card))
    
    
    myResult = cursor.fetchall()
    
    # Convert results to list to allow for creating of df
    
    myResultNew = [list(row) for row in myResult]
    
    
    # dataframe and df list for .iloc() iteration tool thing for file naming
    
    
    df1 = pd.DataFrame(myResultNew, columns = ['CardName', 'CardID','Price', 'PriceDate'])
    
    
    df_list = [df1]
    
    # For loop to name .csv files after each CardID
    
    for df in df_list:
        
        df_id=df['CardID'].iloc[0]
        
        file_name=str(df_id)+'.csv'
        
        df.to_csv(file_name, index=False)

# Close connection to database

cursor.close()

db.close()

print("Connection to DB Closed")


# In[61]:



