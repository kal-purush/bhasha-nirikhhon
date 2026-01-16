import streamlit as st
import numpy as np
import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from sklearn.model_selection import train_test_split

# Sample dataset (for demonstration purposes, replace with actual data)
def generate_sample_data(num_samples=500):
    np.random.seed(42)
    data = pd.DataFrame({
        'Energy Consumption': np.random.uniform(1000, 5000, num_samples),
        'Building Occupancy': np.random.randint(50, 200, num_samples),
        'Maintenance Frequency': np.random.randint(1, 10, num_samples),
        'Service Costs': np.random.uniform(5000, 20000, num_samples)
    })
    return data

# Load and prepare data
data = generate_sample_data()
X = data[['Energy Consumption', 'Building Occupancy', 'Maintenance Frequency']]
y = data['Service Costs']

# Apply Polynomial Features (interaction only)
poly = PolynomialFeatures(degree=2, include_bias=False, interaction_only=True)
X_poly = poly.fit_transform(X)

# Split the data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X_poly, y, test_size=0.2, random_state=42)

# Scaling the data
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Define ideal values for reset
ideal_values = {
    'Energy Consumption': 3000,
    'Building Occupancy': 100,
    'Maintenance Frequency': 5
}

# Streamlit app
st.title("Real Estate & Facility Services Prediction App")

# Initialize session state
if 'energy_slider' not in st.session_state:
    st.session_state['energy_slider'] = ideal_values['Energy Consumption']
if 'occupancy_slider' not in st.session_state:
    st.session_state['occupancy_slider'] = ideal_values['Building Occupancy']
if 'maintenance_slider' not in st.session_state:
    st.session_state['maintenance_slider'] = ideal_values['Maintenance Frequency']

# Reset button
if st.sidebar.button('Reset to Ideal Values'):
    st.session_state['energy_slider'] = ideal_values['Energy Consumption']
    st.session_state['occupancy_slider'] = ideal_values['Building Occupancy']
    st.session_state['maintenance_slider'] = ideal_values['Maintenance Frequency']
    st.rerun()

# Sidebar sliders
energy = st.sidebar.slider('Energy Consumption (kWh)', min_value=1000, max_value=5000, value=st.session_state['energy_slider'], key='energy_slider')
occupancy = st.sidebar.slider('Building Occupancy', min_value=50, max_value=200, value=st.session_state['occupancy_slider'], key='occupancy_slider')
maintenance = st.sidebar.slider('Maintenance Frequency (months)', min_value=1, max_value=12, value=st.session_state['maintenance_slider'], key='maintenance_slider')

# Retrain model and make predictions
user_input = np.array([[energy, occupancy, maintenance]])
user_input_poly = poly.transform(user_input)
user_input_scaled = scaler.transform(user_input_poly)

model = xgb.XGBRegressor(objective='reg:squarederror', random_state=42, reg_alpha=0.1, reg_lambda=0.1)
model.fit(X_train_scaled, y_train)

user_prediction = model.predict(user_input_scaled)
y_pred = model.predict(X_test_scaled)

# Display prediction
st.subheader("Predicted Service Cost:")
st.write(f"${user_prediction[0]:,.2f}")

# Plots
fig, ax = plt.subplots(figsize=(10, 6))
ax.scatter(y_test, y_pred, alpha=0.6, label="Predicted vs Actual")
ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], color='red', linestyle='--', label="Ideal Fit")

# Annotate points near the ideal fit line, avoiding overlaps
distances = abs(y_test - y_pred)
sorted_indices = distances.argsort()
annotated_indices = []
annotated_count = 0

for i in sorted_indices:
    if annotated_count >= 3:
        break
    if i not in annotated_indices:
        overlap = False
        for j in annotated_indices:
            if abs(y_test.iloc[i] - y_test.iloc[j]) < 1000: # Adjust the threshold as needed
                overlap = True
                break
        if not overlap:
            ax.annotate(f"${y_test.iloc[i]:,.0f}\n${y_pred[i]:,.0f}", (y_test.iloc[i], y_pred[i]), textcoords="offset points", xytext=(0, 10), ha='center', fontsize=6)
            annotated_indices.append(i)
            annotated_count += 1

ax.set_title('Predicted vs Actual Values')
ax.set_xlabel('Actual Values')
ax.set_ylabel('Predicted Values')
ax.legend()
st.pyplot(fig)

residuals = y_test - y_pred
fig_res, ax_res = plt.subplots(figsize=(10, 6))
counts, bins, patches = ax_res.hist(residuals, bins=20, color='purple', edgecolor='black')

# Annotate all bars on residuals plot
for count, bin_center in zip(counts, bins[:-1] + np.diff(bins) / 2):
    ax_res.annotate(str(int(count)), xy=(bin_center, count), xytext=(0, 5), textcoords='offset points', ha='center', va='bottom', fontsize=8)

ax_res.set_title('Residuals Distribution')
ax_res.set_xlabel('Residuals (Actual - Predicted)')
ax_res.set_ylabel('Frequency')
st.pyplot(fig_res)

feature_importance = model.feature_importances_
feature_names = poly.get_feature_names_out(X.columns)
sorted_idx = feature_importance.argsort()
fig_feat, ax_feat = plt.subplots(figsize=(12, 6))
ax_feat.barh(feature_names[sorted_idx], feature_importance[sorted_idx])

# Annotate all bars on feature importance plot
for i, v in enumerate(feature_importance[sorted_idx]):
    ax_feat.text(v + 0.01, i, f'{v:.3f}', va='center', ha='left', fontsize=8)

ax_feat.set_title('Feature Importance')
st.pyplot(fig_feat)