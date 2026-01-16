import pandas as pd
import numpy as np
import xgboost as xgb
import pickle

def generate_efficiency_data():
    dates = pd.date_range("2023-01-01", periods=20000)
    temp = 25 + 10 * np.sin(2 * np.pi * (dates.dayofyear - 105) / 365)
    humidity = 60 + 20 * np.cos(2 * np.pi * (dates.dayofyear - 200) / 365)

    data = {
        "Temperature (°C)": np.clip(temp + np.random.normal(0, 3, len(dates)), 15, 45),
        "Humidity (%)": np.clip(humidity + np.random.normal(0, 10, len(dates)), 20, 95),
        "Dust_Level": np.random.choice(["Low", "Medium", "High"], len(dates), p=[0.6, 0.3, 0.1]),
        "Days_Since_Cleaning": np.random.randint(1, 31, len(dates)),
        "Panel_Age (years)": np.random.randint(0, 11, len(dates)),
    }

    df = pd.DataFrame(data, index=dates)

    df["Efficiency (%)"] = (
        92 - 0.5 * df["Panel_Age (years)"] 
        - 0.15 * df["Days_Since_Cleaning"]
        - 3 * (df["Dust_Level"] == "Medium") 
        - 7 * (df["Dust_Level"] == "High")
        - 0.2 * (df["Temperature (°C)"] - 25) ** 2
        + 0.1 * df["Humidity (%)"]
        + np.random.normal(0, 1.5, len(dates))
    )

    return df

df = generate_efficiency_data()
X = pd.get_dummies(df.drop("Efficiency (%)", axis=1))
X["Temp_Humidity"] = X["Temperature (°C)"] * X["Humidity (%)"] / 100
y = df["Efficiency (%)"]

model = xgb.XGBRegressor(
    n_estimators=500,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.7,
    random_state=42
)
model.fit(X, y)

# Save the model and features
with open("solar_model.pkl", "wb") as f:
    pickle.dump((model, X.columns), f)

print("✅ Model trained and saved as 'solar_model.pkl'")