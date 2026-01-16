import pandas as pd
import numpy as np
import streamlit as st
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import classification_report, accuracy_score
from imblearn.over_sampling import SMOTE
import joblib

# Load Dataset
df = pd.read_csv("data.csv", encoding="ISO-8859-1")

# Drop missing values in target column
df.dropna(subset=["Revenue"], inplace=True)

# Select features and target
features = ["Administrative", "Administrative_Duration", "Informational",
            "Informational_Duration", "ProductRelated", "ProductRelated_Duration",
            "BounceRates", "ExitRates", "PageValues", "SpecialDay", "Month",
            "OperatingSystems", "Browser", "Region", "TrafficType", "VisitorType",
            "Weekend"]
X = df[features]
y = df["Revenue"]

# Convert categorical variables to numerical
X = pd.get_dummies(X, drop_first=True)

# Save feature names for later use
feature_columns = X.columns  # Save original column names

# Handle class imbalance
smote = SMOTE(random_state=42)
X_resampled, y_resampled = smote.fit_resample(X, y)

# Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(X_resampled, y_resampled, test_size=0.2, random_state=42)

# Feature Scaling
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Train Model
knn = KNeighborsClassifier(n_neighbors=5, metric="euclidean", weights="distance")
knn.fit(X_train, y_train)

# Save Model & Scaler
joblib.dump(knn, "knn_model.pkl")
joblib.dump(scaler, "scaler.pkl")
joblib.dump(feature_columns, "features.pkl")  # Save feature names

# --------------------------------------------------------------------

# Load trained model and scaler
knn = joblib.load("knn_model.pkl")
scaler = joblib.load("scaler.pkl")  # Save scaler during training

# Streamlit UI
st.title("E-Commerce Purchase Prediction")



# User input fields
admin = st.number_input("Administrative", min_value=0)
admin_dur = st.number_input("Administrative Duration", min_value=0.0)
info = st.number_input("Informational", min_value=0)
info_dur = st.number_input("Informational Duration", min_value=0.0)
product = st.number_input("Product Related", min_value=0)
product_dur = st.number_input("Product Related Duration", min_value=0.0)
bounce = st.number_input("Bounce Rates", min_value=0.0, max_value=1.0)
exit_rate = st.number_input("Exit Rates", min_value=0.0, max_value=1.0)
page_val = st.number_input("Page Values", min_value=0.0)
special_day = st.radio("Special Day", ["No", "Yes"])
special_day_value = 1 if special_day == "Yes" else 0

# Month Encoding
month = st.selectbox("Month", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
month_encoded = {f"Month_{m}": 1 if m == month else 0 for m in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]}

os_type = st.number_input("Operating System", min_value=1)
browser = st.number_input("Browser", min_value=1)
region = st.number_input("Region", min_value=1, value=1)
traffic = st.number_input("Traffic Type", min_value=1)
visitor = st.radio("Visitor Type", ["New_Visitor", "Returning_Visitor"])
visitor_type_value = 1 if visitor == "Returning_Visitor" else 0
weekend = st.radio("Weekend", ["Yes", "No"])
weekend_value = 1 if weekend == "Yes" else 0



# Process user input
if st.button("Predict Purchase"):
    sample_data = {
        "Administrative": admin,
        "Administrative_Duration": admin_dur,
        "Informational": info,
        "Informational_Duration": info_dur,
        "ProductRelated": product,
        "ProductRelated_Duration": product_dur,
        "BounceRates": bounce,
        "ExitRates": exit_rate,
        "PageValues": page_val,
        "SpecialDay": special_day_value,
        "OperatingSystems": os_type,
        "Browser": browser,
        "Region": region,
        "TrafficType": traffic,
        "VisitorType_Returning_Visitor": visitor_type_value,
        "Weekend": weekend_value
    }

    # ✅ Fix: Add Month Encoding to Sample Data
    sample_data.update(month_encoded)

    
    # Convert to DataFrame & match feature columns
    input_df = pd.DataFrame([sample_data])
    input_df = input_df.reindex(columns=X.columns, fill_value=0)

    # Scale input
    input_df_scaled = scaler.transform(input_df)

    # Prediction
    prediction = knn.predict(input_df_scaled)

    # Show result
    if prediction[0] == 1:
        st.success("✅ **Purchase Likely!**")
    else:
        st.error("🚫 **No Purchase Expected!**")