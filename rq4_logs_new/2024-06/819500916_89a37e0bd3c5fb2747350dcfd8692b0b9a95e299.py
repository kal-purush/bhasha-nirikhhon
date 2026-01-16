import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from xgboost import XGBRegressor
from tqdm import tqdm


# 1. DATA PRE PROCESSING
# Cargar los datos
df = pd.read_csv('calendar.csv')

# Convertir la columna de fecha a datetime
df['date'] = pd.to_datetime(df['date'])

# Ordenar por ID y fecha
df.sort_values(by=['listing_id', 'date'], inplace=True)

df['price'] = df['price'].str.replace('$', '').str.replace(',', '').astype(float)

print(df.isnull().sum())

# Después de calcular 'log_price'
df['log_price'] = np.log1p(df['price'])

# Verificar valores infinitos o NaN
print("Valores infinitos en log_price:", np.isinf(df['log_price']).sum())
print("Valores NaN en log_price:", np.isnan(df['log_price']).sum())

# Eliminar filas con valores infinitos o NaN en log_price
df = df[~np.isinf(df['log_price'])]
df = df.dropna(subset=['log_price'])

# Verificar valores muy grandes
print("Valor máximo de log_price:", df['log_price'].max())
print("Valor mínimo de log_price:", df['log_price'].min())

# Si es necesario, puedes recortar valores extremos
# df['log_price'] = np.clip(df['log_price'], lower_bound, upper_bound)

# Extraer características de la fecha
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['day'] = df['date'].dt.day
df['dayofweek'] = df['date'].dt.dayofweek
df['is_weekend'] = df['dayofweek'].isin([5, 6]).astype(int)
df['quarter'] = df['date'].dt.quarter
df['is_holiday'] = ((df['month'] == 12) & (df['day'] >= 20)) | (df['month'] == 1) & (df['day'] <= 5)
df['days_since_start'] = (df['date'] - df['date'].min()).dt.days



# 1.2. GENERATING NEW VARIABLES FROM PRE-EXISTING ONES

# Calcular la media del precio para cada Airbnb
airbnb_mean_prices = df.groupby('listing_id')['price'].mean()

# Añadir la media del precio como una característica
df['mean_airbnb_price'] = df['listing_id'].map(airbnb_mean_prices)

# Añadir característica: precio del año anterior en el mismo día y mes
df['last_year_price'] = df.groupby(['listing_id', df['date'].dt.month, df['date'].dt.day])['price'].shift()

# En lugar de eliminar filas, llenamos los NaN con la media del precio del Airbnb
df['last_year_price'] = df['last_year_price'].fillna(df['mean_airbnb_price'])

# 1.3. Merging with visualization data for neighbourhood data

listings = pd.read_csv('listings_redux.csv')

merged_data = pd.merge(df, listings[['listing_id', 'neighbourhood']], on='listing_id', how='left')
merged_data.to_csv('cal_listings_merged.csv', index=False)


# 2. DEFINE PREDICTINIG FEATURES AND PREDICTED TARGET VALUE
features = ['year', 'month', 'day', 'dayofweek', 'is_weekend', 'quarter', 'is_holiday', 'days_since_start', 'last_year_price', 'mean_airbnb_price']
target = 'log_price'

X = df[features]
Y = df[target]

# 3.  TRAIN THE MODEL WITH K-FOLD CROSS VALIDATION AND SAVE THE MODEL
import pickle
# Crear el pipeline de preprocesamiento
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), features)
    ])

# Crear el modelo XGBoost
model = XGBRegressor(
    n_estimators=1000,
    learning_rate=0.01,
    max_depth=7,
    min_child_weight=3,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    verbose=1  # Para ver el progreso
)

# Crear el pipeline completo
pipeline = Pipeline([
    ('preprocessor', preprocessor),
    ('regressor', model)
])

# Usar validación cruzada con series temporales
tscv = TimeSeriesSplit(n_splits=5)
mse_scores = []
r2_scores = []
mae_scores = []

for train_index, test_index in tqdm(tscv.split(X), total=tscv.n_splits, desc="Cross-validation"):
    X_train, X_test = X.iloc[train_index], X.iloc[test_index]
    y_train, y_test = y.iloc[train_index], y.iloc[test_index]
    
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    
    mse_scores.append(mean_squared_error(y_test, y_pred))
    r2_scores.append(r2_score(y_test, y_pred))
    mae_scores.append(mean_absolute_error(y_test, y_pred))

# Calcular y mostrar las métricas promedio
mse = np.mean(mse_scores)
rmse = np.sqrt(mse)
r2 = np.mean(r2_scores)
mae = np.mean(mae_scores)

print(f"Error cuadrático medio (MSE): {mse:.4f}")
print(f"Raíz del error cuadrático medio (RMSE): {rmse:.4f}")
print(f"R-cuadrado: {r2:.4f}")
print(f"Error absoluto medio (MAE): {mae:.4f}")

# Convertir el RMSE y MAE de vuelta a la escala original
print(f"RMSE en la escala original: ${np.expm1(rmse):.2f}")
print(f"MAE en la escala original: ${np.expm1(mae):.2f}")

# Imprimir las características más importantes
feature_importance = model.feature_importances_
sorted_idx = np.argsort(feature_importance)
for idx in sorted_idx:
    print(f"{features[idx]}: {feature_importance[idx]:.4f}")


# 4. SAVE THE MODEL
# Once we determined how good our model was for predicting the data, we train it on 
# the main dataset and save it 
pipeline.fit(X, Y)
# To save a model:
with open('our_estimator.pkl', 'wb') as fid:
    pickle.dump(pipeline, fid)



from tqdm import tqdm
import pandas as pd
import numpy as np

# 5. TEST OF INPUT FEATURES FOR THE MODEL, MIMICKING USER INPUT IN WEB
# Part of the prediction process in the main file


# Supongamos que ya tienes tu modelo 'pipeline' y tus datos 'df' cargados y preparados
# To deserialize estimator later
with open('our_estimator.pkl', 'rb') as fid:
    pp = pickle.load(fid)

# Obtén la fecha de inicio para las predicciones (hoy + 1 año)
start_date = pd.Timestamp.today().normalize()

# Crea un rango de fechas futuras a partir de start_date
future_dates = pd.date_range(start=start_date, periods=365, freq='D')

# Convierte future_dates a un DataFrame y extrae año, mes, día, etc.
future_dates_df = pd.DataFrame({'date': future_dates})
future_dates_df['year'] = future_dates_df['date'].dt.year
future_dates_df['month'] = future_dates_df['date'].dt.month
future_dates_df['day'] = future_dates_df['date'].dt.day
future_dates_df['dayofweek'] = future_dates_df['date'].dt.dayofweek
future_dates_df['is_weekend'] = future_dates_df['dayofweek'].isin([5, 6]).astype(int)
future_dates_df['quarter'] = future_dates_df['date'].dt.quarter
future_dates_df['is_holiday'] = ((future_dates_df['month'] == 12) & (future_dates_df['day'] >= 20)) | ((future_dates_df['month'] == 1) & (future_dates_df['day'] <= 5))
future_dates_df['days_since_start'] = (future_dates_df['date'] - df['date'].min()).dt.days

# Calcula la media del precio para cada Airbnb
airbnb_mean_prices = df.groupby('listing_id')['price'].mean()

# Crea un DataFrame con las fechas futuras replicando cada Airbnb para cada fecha
airbnb_ids = df['listing_id'].unique()
future_data = pd.DataFrame()

# Usa tqdm para la barra de progreso
for airbnb_id in tqdm(airbnb_ids, desc="Predicting prices", total=len(airbnb_ids)):
    airbnb_data = future_dates_df.copy()
    airbnb_data['airbnb_id'] = airbnb_id
    
    # Prepara los datos del año pasado
    past_data = df[df['listing_id'] == airbnb_id].copy()
    past_data['month_day'] = past_data['date'].dt.strftime('%m-%d')
    
    # Merge para agregar la característica del precio del año pasado en el mismo día y mes
    airbnb_data['month_day'] = airbnb_data['date'].dt.strftime('%m-%d')
    merged_data = pd.merge(airbnb_data, past_data[['month_day', 'price']], on='month_day', how='left')
    airbnb_data['last_year_price'] = merged_data['price'].fillna(airbnb_mean_prices[airbnb_id])
    
    # Añade la media del precio del Airbnb como una característica
    airbnb_data['mean_airbnb_price'] = airbnb_mean_prices[airbnb_id]
    
    # Asegúrate de que todas las características necesarias estén presentes
    for feature in features:
        if feature not in airbnb_data.columns:
            airbnb_data[feature] = 0  # o cualquier otro valor por defecto apropiado
    
    # Hacer predicciones para este Airbnb y las fechas futuras
    predicted_log_prices = pp.predict(airbnb_data[features])
    
    # Convertir el logaritmo del precio predicho a la escala original
    predicted_prices = np.expm1(predicted_log_prices)
    
    # Agregar las predicciones al DataFrame de fechas futuras
    airbnb_data['predicted_price'] = predicted_prices
    
    # Concatenar los resultados al DataFrame principal
    future_data = pd.concat([future_data, airbnb_data], ignore_index=True)

# Mostrar las predicciones para todas las combinaciones de Airbnb y fechas futuras
print(future_data[['airbnb_id', 'date', 'predicted_price']])



