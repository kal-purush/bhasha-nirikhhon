import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from tensorflow import keras
from tensorflow.keras import layers

# Streamlit UI
st.title("🚀 Employee Promotion Prediction")

# Upload CSV File
uploaded_file = st.file_uploader("📂 Upload your dataset (CSV file)", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

    st.subheader("📊 Dataset Preview")
    st.write(df.head())

    # Drop employee_id if it exists
    if "employee_id" in df.columns:
        df.drop(columns=["employee_id"], inplace=True)

    # Fill missing values
    df["education"].fillna(df["education"].mode()[0], inplace=True)
    df["previous_year_rating"].fillna(df["previous_year_rating"].median(), inplace=True)

    # Separate features and target
    X = df.drop(columns=["is_promoted"])
    y = df["is_promoted"]

    # One-hot encode categorical columns
    categorical_cols = ["department", "region", "education", "gender", "recruitment_channel"]
    encoder = OneHotEncoder(drop="first", sparse=False)
    X_encoded = encoder.fit_transform(X[categorical_cols])

    # Convert numerical features to numpy array
    numerical_cols = ["no_of_trainings", "age", "previous_year_rating", "length_of_service", "awards_won?", "avg_training_score"]
    X_numerical = X[numerical_cols].values

    # Standardize numerical data
    scaler = StandardScaler()
    X_numerical_scaled = scaler.fit_transform(X_numerical)

    # Combine numerical and encoded categorical features
    X_processed = np.hstack((X_numerical_scaled, X_encoded))

    # Split into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X_processed, y, test_size=0.2, random_state=42)

    # Build Neural Network Model
    model = keras.Sequential([
        layers.Dense(64, activation="relu", input_shape=(X_train.shape[1],)),
        layers.Dense(32, activation="relu"),
        layers.Dense(1, activation="sigmoid")
    ])

    model.compile(optimizer="adam", loss="binary_crossentropy", metrics=['accuracy'])

    # Train the model
    st.subheader("⚙️ Training Model...")
    with st.spinner("Training in progress..."):
        model.fit(X_train, y_train, batch_size=32, epochs=5, validation_split=0.2, verbose=1)

    # Evaluate model on test set
    test_loss, test_acc = model.evaluate(X_test, y_test)
    predictions = model.predict(X_test)
    y_pred = (predictions > 0.5).astype(int).flatten()

    st.success(f"✅ Test Accuracy: {test_acc:.4f}")

    # Convert predictions into DataFrame
    results_df = pd.DataFrame({"Actual": y_test.values, "Predicted": y_pred})

    # Interactive Plot
    st.subheader("📈 Actual vs Predicted Promotions")
    fig = px.scatter(results_df, x="Actual", y="Predicted", color="Actual",
                     title="Comparison of Actual and Predicted Promotions",
                     labels={"Actual": "Actual Promotion", "Predicted": "Predicted Promotion"})
    st.plotly_chart(fig)

    # Filter employees predicted for promotion
    promoted_employees = df.iloc[y_test.index]
    promoted_employees["Predicted_Promotion"] = y_pred

    # Allow CSV download of results
    st.subheader("⬇️ Download Predicted Promotions")
    csv = promoted_employees.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", csv, "predicted_promotions.csv", "text/csv", key="download-csv")

    # Display the first few predicted promotions
    st.write("📝 Employees predicted to be promoted:")
    st.write(promoted_employees.head())