import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix

# Load the dataset
hotel_raw = pd.read_csv('hotel_bookings.csv', header=0)

# Preprocess data, handle missing values, and convert categorical variables

# Select relevant features
features = ['lead_time', 'arrival_date_year', 'total_of_special_requests', 'adr']

# One-hot encode categorical variable 'customer_type'
X = pd.get_dummies(hotel_raw[features + ['customer_type']], drop_first=True)
y = hotel_raw['is_canceled']

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize and train the Random Forest model
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate the model
print("Confusion Matrix:")
print(confusion_matrix(y_test, y_pred))
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

# Calculate total special requests and average special requests
special_requests_info = hotel_raw['total_of_special_requests'].agg(['sum', 'mean'])

# Print the special requests information
print("\nTotal Special Requests and Average Special Requests:")
print(special_requests_info)

# Analyze average daily rate for the last 3 months
hotel_raw['arrival_date'] = pd.to_datetime(hotel_raw['arrival_date_year'].astype(str) + '-' + hotel_raw['arrival_date_month'] + '-01')
last_3_months = (hotel_raw['arrival_date'] >= '2017-06-01') & (hotel_raw['arrival_date'] <= '2017-08-31')
average_daily_rate_last_3_months = hotel_raw.loc[last_3_months, 'adr'].mean()

# Print the average daily rate for the last 3 months
print("\nAverage Daily Rate for Last 3 Months:", average_daily_rate_last_3_months)

# Calculate total revenue lost due to cancellations
total_cancellations = hotel_raw['is_canceled'].sum()
average_daily_rate = hotel_raw['adr'].mean()
total_revenue_lost = total_cancellations * average_daily_rate

# Print the total revenue lost
print("Total Revenue Lost due to Cancellations:", total_revenue_lost)