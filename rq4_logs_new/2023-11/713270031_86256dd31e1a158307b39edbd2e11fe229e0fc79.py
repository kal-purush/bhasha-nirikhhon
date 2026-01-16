
pca = """ 
import pandas as pd 
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

df = pd.read_csv('Wine.csv')
df

df.keys()

print(df.isnull().sum()) 

X = df.drop('Customer_Segment', axis=1) # Features
y = df['Customer_Segment'] 

sc = StandardScaler() #Standardize features by removing the mean and scaling to 

mean=0 
Stddeviation=1
X = sc.fit_transform(X)
X.head(5)

pca = PCA()
X_pca = pca.fit_transform(X)

explained_variance_ratio = pca.explained_variance_ratio_
plt.plot(range(1, len(explained_variance_ratio) + 1), explained_variance_ratio.cumsum(), marker='o', 
linestyle='--')
plt.xlabel('Number of Principal Components')
plt.ylabel('Cumulative Explained Variance')
plt.title('Explained Variance Ratio')
plt.show()

n_components = 12 # Choose the desired number of principal components you want to reduce a dimention to
pca = PCA(n_components=n_components)
X_pca = pca.fit_transform(X)
X_pca.shape
X.shape
red_indices = y[y == 1].index
white_indices = y[y == 2].index

plt.scatter(X_pca[red_indices, 0], X_pca[red_indices, 1], c='red', label='Red Wine')
plt.scatter(X_pca[white_indices, 0], X_pca[white_indices, 1], c='blue', label='White Wine')
plt.quiver(0, 0, 0.5, 0, angles='xy', scale_units='xy', scale=1, color='g', label='Principal Component 1')
plt.quiver(0, 0, 0, 0.5, angles='xy', scale_units='xy', scale=1, color='m', label='Principal Component 2')

plt.xlabel('Principal Component 1')
plt.ylabel('Principal Component 2')
plt.legend()
plt.title('PCA: Red Wine vs. White Wine')
plt.show()
"""

uber_ride = """
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.impute import SimpleImputer  # Import SimpleImputer
import seaborn as sns
import matplotlib.pyplot as plt

data = pd.read_csv('uber.csv')  # Replace with the actual path to your dataset.

data['pickup_datetime'] = pd.to_datetime(data['pickup_datetime'])

data['hour'] = data['pickup_datetime'].dt.hour
data['day_of_week'] = data['pickup_datetime'].dt.dayofweek

data = data.drop(columns=['Unnamed: 0', 'key', 'pickup_datetime'])

imputer = SimpleImputer(strategy='mean')
data_imputed = pd.DataFrame(imputer.fit_transform(data), columns=data.columns)

X = data_imputed.drop(columns=['fare_amount'])
y = data_imputed['fare_amount']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

plt.figure(figsize=(8, 6))
sns.boxplot(data=data, x='fare_amount')
plt.title('Box Plot of Fare Amount')
plt.show()

correlation_matrix = data.corr()
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm")
plt.title('Correlation Matrix Heatmap')
plt.show()

# Linear Regression
lr = LinearRegression()
lr.fit(X_train, y_train)
y_pred_lr = lr.predict(X_test)
r2_lr = r2_score(y_test, y_pred_lr)
rmse_lr = np.sqrt(mean_squared_error(y_test, y_pred_lr))

# Ridge Regression
ridge = Ridge(alpha=1.0)  # You can adjust the alpha parameter
ridge.fit(X_train, y_train)
y_pred_ridge = ridge.predict(X_test)
r2_ridge = r2_score(y_test, y_pred_ridge)
rmse_ridge = np.sqrt(mean_squared_error(y_test, y_pred_ridge))

# Lasso Regression
lasso = Lasso(alpha=1.0)  # You can adjust the alpha parameter
lasso.fit(X_train, y_train)
y_pred_lasso = lasso.predict(X_test)
r2_lasso = r2_score(y_test, y_pred_lasso)
rmse_lasso = np.sqrt(mean_squared_error(y_test, y_pred_lasso))

# Print results
print("Linear Regression - R2:", r2_lr, "RMSE:", rmse_lr)
print("Ridge Regression - R2:", r2_ridge, "RMSE:", rmse_ridge)
print("Lasso Regression - R2:", r2_lasso, "RMSE:", rmse_lasso)
"""

svm_digit_image = """
from sklearn.datasets import fetch_openml
mnist = fetch_openml('mnist_784', version=1)
X, y = mnist.data, mnist.target.astype(int)

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

from sklearn.svm import SVC
svm_classifier = SVC(kernel='linear')
svm_classifier.fit(X_train, y_train)

from sklearn.metrics import accuracy_score
y_pred = svm_classifier.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)

import matplotlib.pyplot as plt
import numpy as np

n_samples_to_visualize = 10
random_indices = np.random.randint(0, len(X_test), n_samples_to_visualize)

predicted_labels = svm_classifier.predict(X_test[random_indices])

plt.figure(figsize=(12, 6))
for i, idx in enumerate(random_indices):
    plt.subplot(2, 5, i + 1)
    plt.imshow(X_test[idx].reshape(28, 28), cmap='gray')
    plt.axis('off')

plt.tight_layout()
plt.show()
"""

K_Means_elbow_iris = """
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
from sklearn.cluster import KMeans 
import seaborn as sns

data = pd.read_csv('Iris.csv')
data

data.columns


for i,col in enumerate(data.columns):
    print(f'Column number {1+i} is {col}')

data.dtypes

data.drop('Id', axis=1, inplace=True)
data.head()

data.isna().sum()

target_data = data.iloc[:,4]
target_data.unique()

clustering_data = data.iloc[:,[0,1,2,3]]
clustering_data.head()

fig, ax = plt.subplots(figsize=(15,7))
sns.set(font_scale=1.5)
ax = sns.scatterplot(x=data['SepalLengthCm'],y=data['SepalWidthCm'], s=70, color='#f73434',
edgecolor='#f73434', linewidth=0.3)
ax.set_ylabel('Sepal Width (in cm)')
ax.set_xlabel('Sepal Length (in cm)')
plt.title('Sepal Length vs Width', fontsize = 20)
plt.show()

from sklearn.cluster import KMeans
wcss=[]
for i in range(1,11):
    km = KMeans(i)

km.fit(clustering_data)
wcss.append(km.inertia_)
np.array(wcss)

kms = KMeans(n_clusters=3, init='k-means++')
kms.fit(clustering_data)
KMeans(n_clusters=3)

clusters = clustering_data.copy()
clusters['Cluster_Prediction'] = kms.fit_predict(clustering_data)
clusters.head()

kms.cluster_centers_

import plotly.express as px

cluster0 = clusters[clusters['Cluster_Prediction'] == 0]
cluster1 = clusters[clusters['Cluster_Prediction'] == 1]
cluster2 = clusters[clusters['Cluster_Prediction'] == 2]

fig = px.scatter(clusters, x='SepalLengthCm', y='SepalWidthCm', color='Cluster_Prediction',
                 size_max=30, opacity=0.7, title='Clusters', labels={'SepalLengthCm': 'Sepal Length (in cm)', 'SepalWidthCm': 'Sepal Width (in cm)'})

fig.add_scatter(x=kms.cluster_centers_[:, 0], y=kms.cluster_centers_[:, 1], 
                mode='markers', marker=dict(size=20, color='yellow', line=dict(color='black', width=1)),
                name='Centroids')

fig.for_each_trace(lambda t: t.update(name='Cluster ' + str(t.name)))

fig.update_layout(legend_title_text='Clusters', xaxis_range=[4, 8], yaxis_range=[1.8, 4.5], xaxis_title='Sepal Length (in cm)', yaxis_title='Sepal Width (in cm)')
fig.show()

# pip install nbformat>=4.2.0
"""

Random_Forest_Classifier_safety = """ 
import pandas as pd
df=pd.read_csv('car_evaluation.csv')
col_names = ['buying', 'maint', 'doors', 'persons', 'lug_boot', 'safety', 'class']

df.columns=col_names
col_name

for col in col_names:
    print(df[col].value_counts())

x=df.drop(['class'],axis=1)
y=df['class']

from sklearn.model_selection import train_test_split
x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.3,random_state=42)

import category_encoders as ce

encoder = ce.OrdinalEncoder(cols=['buying', 'maint', 'doors', 'persons', 'lug_boot', 'safety'])
x_train = encoder.fit_transform(x_train)
x_test = encoder.transform(x_test)

from sklearn.ensemble import RandomForestClassifier

rfc=RandomForestClassifier(random_state=0)

rfc.fit(x_train,y_train)

y_pred=rfc.predict(x_test)

from sklearn.metrics import accuracy_score
accuracy_score(y_test,y_pred)

rfc_100 = RandomForestClassifier(n_estimators=100, random_state=0)
rfc_100.fit(x_train, y_train)

y_pred_100=rfc_100.predict(x_test)
accuracy_score(y_test,y_pred_100)

from sklearn.ensemble import RandomForestClassifier
rfc_100=RandomForestClassifier(n_estimators=100,random_state=0)
rfc_100.fit(x_train,y_train)
y_pred_100 = rfc_100.predict(x_test)
print("model accuracy n_estimator=100: {0:0.4f}".format(accuracy_score(y_test,y_pred_100)))

print(y_train)
"""

reinforcement_learning: """
import numpy as np

def create_maze():
    rows = int(input("Enter the number of rows: "))
    cols = int(input("Enter the number of columns: "))
    maze = np.zeros((rows, cols), dtype=int)
    print("Enter the maze layout:")
    for row in range(rows):
        row_data = input().strip()
        maze[row] = [int(cell) for cell in row_data]
    return maze
maze = create_maze()
class QLearningAgent:
    def __init__(self, num_states, num_actions, learning_rate=0.1, discount_factor=0.9, exploration_prob=0.2):
        self.num_states = num_states
        self.num_actions = num_actions
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.exploration_prob = exploration_prob
        self.q_table = np.zeros((num_states, num_actions))

    def choose_action(self, state):
        if np.random.rand() < self.exploration_prob:
            return np.random.choice(self.num_actions)  
        else:
            return np.argmax(self.q_table[state])  

    def learn(self, state, action, reward, next_state):
        predicted = self.q_table[state, action]
        target = reward + self.discount_factor * np.max(self.q_table[next_state])
        self.q_table[state, action] += self.learning_rate * (target - predicted)
num_states = maze.size
num_actions = 4  

initial_state = 0
goal_state = num_states - 1

agent = QLearningAgent(num_states, num_actions)
def train_agent(agent, num_episodes=1000):
    for episode in range(num_episodes):
        state = initial_state
        done = False
        while not done:
            action = agent.choose_action(state)
            next_state = state
            if action == 0:  # Move Up
                next_state = state - maze.shape[1]
            elif action == 1:  # Move Down
                next_state = state + maze.shape[1]
            elif action == 2:  # Move Left
                next_state = state - 1
            elif action == 3:  # Move Right
                next_state = state + 1

            if (0 <= next_state < num_states) and (maze.flat[next_state] == 0):  # Check if the move is valid
                if next_state == goal_state:
                    reward = 1  # Reached the goal
                    done = True
                else:
                    reward = 0  # Moved to an empty cell
                agent.learn(state, action, reward, next_state)
                state = next_state

train_agent(agent, num_episodes=1000)

def test_agent(agent):
    state = initial_state
    while state != goal_state:
        action = agent.choose_action(state)
        print(f"Current State: {state}, Chosen Action: {action}")
        if action == 0:
            state = state - maze.shape[1]
        elif action == 1:
            state = state + maze.shape[1]
        elif action == 2:
            state = state - 1
        elif action == 3:
            state = state + 1
        print(f"New State: {state}")
    print("Agent reached the goal!")

test_agent(agent)

"""

data_load_store_file = """
import pandas as pd

# csv
df = pd.read_csv('Dummy Data HSS.csv')

# excel
# df_excel = pd.read_excel('sales_data.xlsx')

# json
# import json
# with open('sales.json') as f:
#     data_json = json.load(f)
# df_json = pd.DataFrame(data_json)

df.head()

df.info()

df.isnull().sum()

value = df['TV'].mean()
df['TV'].fillna(value, inplace=True)
# df.drop_duplicates(inplace=True)

df.isnull().sum()

df.describe()

category_sales = df.groupby(['TV', 'Radio', 'Social Media'])['Sales'].sum()
category_sales

total_sales = df['Sales'].sum()
print('total sales',total_sales)
avg_order_value = df['Sales'].mean()
print('avg order value',avg_order_value)

import matplotlib.pyplot as plt

channels = ['TV', 'Radio', 'Social Media']

sales = df[['TV', 'Radio', 'Social Media']].sum()

import plotly.express as px

channels = ['TV', 'Radio', 'Social Media']

sales = df[['TV', 'Radio', 'Social Media']].sum()

px.bar(x=channels, y=sales)
"""

openweatherAPI = """
import requests
import pandas as pd
import datetime
#OpenWeatherMap API key
api_key = 'fb365aa6104829b44455572365ff3b4e'

lat = 18.184135
lon = 74.610764
api_url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}"
response = requests.get(api_url)
weather_data = response.json() #pass response to weather_data object(dictionary)
weather_data.keys()
dict_keys=(['cod', 'message', 'cnt', 'list', 'city'])
weather_data['list'][0]
{'dt': 1690189200,
 'main': {'temp': 298.21,
 'feels_like': 298.81,
 'temp_min': 298.1,
 'temp_max': 298.21,
 'pressure': 1006,
 'sea_level': 1006,
 'grnd_level': 942,
 'humidity': 78,
 'temp_kf': 0.11},
 'weather': [{'id': 804,
 'main': 'Clouds',
 'description': 'overcast clouds',
 'icon': '04d'}],
 'clouds': {'all': 100},
 'wind': {'speed': 6.85, 'deg': 258, 'gust': 12.9},
 'visibility': 10000,
 'pop': 0.59,
 'sys': {'pod': 'd'},
 'dt_txt': '2023-07-24 09:00:00'}

len(weather_data['list'])

weather_data['list'][0]['weather'][0]['description']

temperatures = [item['main']['temp'] for item in weather_data['list']]
timestamps = [pd.to_datetime(item['dt'], unit='s') for item in weather_data['list']]
temperature = [item['main']['temp'] for item in weather_data['list']]
humidity = [item['main']['humidity'] for item in weather_data['list']]
wind_speed = [item['wind']['speed'] for item in weather_data['list']]
weather_description = [item['weather'][0]['description'] for item in weather_data['list']]
# Create a pandas DataFrame with the extracted weather data
weather_df = pd.DataFrame({
'Timestamp': timestamps,
'Temperature': temperatures,
'humidity': humidity,
'wind_speed': wind_speed,
'weather_description': weather_description,
})
# Set the Timestamp column as the DataFrame's index
weather_df.set_index('Timestamp', inplace=True)
max_temp = weather_df['Temperature'].max()
max_temp

min_temp = weather_df['Temperature'].min()
min_temp

# Handling missing values
weather_df.fillna(0, inplace=True) # Replace missing values with 0 or appropriate value
# Handling inconsistent format (if applicable)
weather_df['Temperature'] = weather_df['Temperature'].apply(lambda x: x - 273.15 if isinstance(x, float)else x) 

print(weather_df)
"""

dc_customer_churn = """
import pandas as pd #data manipulation
import numpy as np #numerical computations
from sklearn.model_selection import train_test_split 
from sklearn import metrics #evaluating the performance of machine learning model

data = pd.read_csv("Telecom_Customer_Churn.csv")
print(data.index)

print(data.columns)

data.shape

print(data.head())

data.isna().sum()

print("Number of rows before removing duplicates:", len(data))

# Remove duplicate records
data_cleaned = data.drop_duplicates()

# Check the number of rows after removing duplicates
print("Number of rows after removing duplicates:", len(data_cleaned))

data.describe()

#Measure of frequency destribution
unique, counts = np.unique(data['Tenure in Months'], return_counts=True)
print(unique, counts)

unique, counts = np.unique(data['Total Charges'], return_counts=True)
print(unique, counts)

import seaborn as sns #Seaborn library for data visualization
sns.pairplot(data)

X = data.drop("Total Revenue", axis=1)
y = data["Total Revenue"]
# Split the dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

X_train.shape

y_train.shape

X_test.shape

y_test.shape

# Export the cleaned dataset to a CSV file
data.to_csv("Cleaned_Telecom_Customer_Churn.csv", index=False)
"""

dw_real_esatate_market = """
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
%matplotlib inline
import matplotlib
matplotlib.rcParams["figure.figsize"] = (20,10)

df1 = pd.read_csv("Bengaluru_House_Data.csv")
df1.head()

df1.shape

df1.columns

df1['area_type']

df1['area_type'].unique()

df1['area_type'].value_counts()

df2 = df1.drop(['area_type','society','balcony','availability'],axis='columns')
df2.shape

df2.isnull().sum()

df2.shape

df3 = df2.dropna()
df3.isnull().sum()

df3.shape

df3['size'].unique()

df3['bhk'] = df3['size'].apply(lambda x: int(x.split(' ')[0]))

 df3['bhk'] = df3['size'].apply(lambda x: int(x.split(' ')[0]))
df3.head()

df3.bhk.unique()

df3[df3.bhk>20]

df3.total_sqft.unique()
"""

visualize_AQI = """
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv("data/data.csv",encoding='latin1')
print(data.index)

sns.set(style="ticks", rc={'figure.figsize': (20, 15)})

print(data.isnull().sum())
print(data.shape)
data.info()

print(data.isnull().sum())
data.tail()


state_means = data.groupby('state')['no2'].mean()

x_axis = state_means.index
y_axis = state_means.values

plt.figure(figsize=(12, 6))
plt.bar(x_axis, y_axis, color='blue')
plt.xlabel('State')
plt.ylabel('Mean NO2 Value')
plt.title('Mean NO2 Values by State')
plt.xticks(rotation=90)  
plt.tight_layout()
plt.show()

state_means = data.groupby('state')['so2'].mean()

x_axis = state_means.index
y_axis = state_means.values

plt.figure(figsize=(12, 6))
plt.bar(x_axis, y_axis, color='blue')
plt.xlabel('State')
plt.ylabel('Mean SO2 Value')
plt.title('Mean SO2 Values by State')
plt.xticks(rotation=90)  
plt.tight_layout()
plt.show()
"""

customer_shopping = """
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

df= pd.read_csv("customer_shopping_data.csv\customer_shopping_data.csv")
df.head()

df.groupby("shopping_mall").count()

df.groupby("category").count()

branch_sales = df.groupby("shopping_mall").sum()

category_sales = df.groupby("category").sum()

branch_sales.sort_values(by = "price", ascending = False)

category_sales.sort_values(by = "price", ascending = False)

combined_branch_category_sales = df.groupby(["shopping_mall", "category"]).sum()
combined_branch_category_sales
# pie chart for sales by branch
plt.Figure(figsize=(12,21))
plt.pie(branch_sales["price"], labels = branch_sales.index)
plt.show()

plt.pie(category_sales["price"], labels = category_sales.index)
plt.show()

combined_pivot = df.pivot_table(index="shopping_mall", columns="category", values="price", aggfunc="sum")
# grouped bar chart for sales of different categories at different branches
combined_pivot.plot(kind="bar", figsize=(10, 6))
plt.show()
"""

analysis_stockMarket = """
import pandas as pd 
import tensorflow as tf 
import numpy as np
#Creating dataframe
dfcsv = pd.read_csv('sales_data_sample 2.csv', encoding = "latin") 
#Printing first 5 rows of dataset
dfcsv.head(5)

dfcsv.shape

dfcsv.isna().sum()

dfcsv.describe

dfcsv = dfcsv.drop(['ADDRESSLINE1','ADDRESSLINE2','CITY','STATE','TERRITORY'],axis = 1)

dfcsv.isna().sum()

dfcsv = dfcsv['POSTALCODE'].fillna(dfcsv.POSTALCODE.mode(), inplace=True)
columns_to_drop = ['ADDRESSLINE1', 'ADDRESSLINE2', 'CITY', 'STATE', 'TERRITORY']

import plotly.express as px

fig = px.bar(df, x='YEAR_ID', y='SALES', title='Total Sales by Year')
fig.show()

fig = px.bar(df, x='QTR_ID', y='SALES', title='Total Sales by Quarter')
fig.show()
"""