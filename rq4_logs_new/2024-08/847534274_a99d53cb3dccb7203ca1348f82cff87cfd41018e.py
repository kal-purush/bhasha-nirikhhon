import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
#Reading the file
df= pd.read_csv('t20.csv')
print(df.head())

#Checking if there is null
print(df.isnull().sum())
data = df.dropna()

#For label encoder for date have to break it 
df['Date'] = pd.to_datetime(df['Date'])
df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
df['Day'] = df['Date'].dt.day

#Making X and y for input and output
df.drop(columns=['Date'], inplace=True)
X = df.drop('Winner', axis='columns')
y = df['Winner']

#Using label encoder to convert
label_Venue = LabelEncoder()
label_Bat_F = LabelEncoder()
label_Bat_S = LabelEncoder()
label_Year = LabelEncoder()
label_Month = LabelEncoder()
label_Day = LabelEncoder()
label_Winner = LabelEncoder()

#Using label encoder to convert
X['Venue_new'] = label_Venue.fit_transform(X['Venue'])
X['Bat_First_new'] = label_Bat_F.fit_transform(X['Bat_First'])
X['Bat_Second_new'] = label_Bat_S.fit_transform(X['Bat_Second'])
X['Year_new'] = label_Year.fit_transform(X['Year'])
X['Month_new'] = label_Month.fit_transform(X['Month'])
X['Day_new'] = label_Day.fit_transform(X['Day'])

#New columns for the numeric data
X_new = X.drop(['Venue','Bat_First','Bat_Second','Year','Month','Day'], axis='columns')
print(X_new)

model = DecisionTreeClassifier()
#Train and test split used for data

X_train, X_test, y_train, y_test = train_test_split(X_new, y, test_size=0.2, random_state=10)
#Fit function used
model.fit(X_train, y_train)
y_pred_test = model.predict(X_test)
y_pred_train = model.predict(X_train)

#Prediction for the given data
prediction = model.predict([[157, 22, 2, 0, 5, 12]])
print("Prediction:", prediction)

#Accuracy Score
score = accuracy_score(y_train, y_pred_train)
print("Model Accuracy Train:", score)
score_test = accuracy_score(y_test, y_pred_test)
print("Model Accuracy Test:", score_test)