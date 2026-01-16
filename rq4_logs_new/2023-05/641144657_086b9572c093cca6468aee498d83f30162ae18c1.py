import sqlite3
import pandas as pd

df_diabetes = pd.read_csv('diabetes_binary_health_indicators_BRFSS2015.csv')

conn = sqlite3.connect('diabetes_binary_health_indicators_BRFSS2015.sqlite')

conn.execute('''CREATE TABLE diabetes_binary (
    Diabetes INT,
    HighBP INT,
    HighChol INT,
    CholCheck INT,
    BMI INT,
    Smoker INT,
    Stroke INT,
    HeartDiseaseorAttack INT,
    PhysActivity INT,
    Fruits INT,
    Veggies INT,
    HvyAlcoholConsump INT,
    AnyHealthcare INT,
    NoDocbcCost INT,
    GenHlth INT,
    MentHlth INT,
    PhysHlth INT,
    DiffWalk INT,
    Sex INT,
    Age INT,
    Education INT,
    Income INT
)''')

df_diabetes.to_sql("diabetes_binary", conn, if_exists='replace', index=False)

query = "SELECT * FROM diabetes_binary"
df_diabetes = pd.read_sql(query, conn)

conn.commit()
conn.close()

No_Health_Insurance = pd.read_csv('No_Health_Insurance.csv')

conn = sqlite3.connect('No_Health_Insurance.sqlite')

conn.execute('''CREATE TABLE no_health_insurance (
    Year INT,
    County_FIPS INT,
    "County" VARCHAR,
    "State" VARCHAR,
    "Diagnosed Diabetes Percentage" FLOAT,
    "No Health Insurance" FLOAT
)''')

No_Health_Insurance.to_sql("no_health_insurance", conn, if_exists='replace', index=False)

query = "SELECT * FROM no_health_insurance"
No_Health_Insurance = pd.read_sql(query, conn)

conn.commit()
conn.close()

Food_Insecurity = pd.read_csv('Food_Insecurity.csv')

conn = sqlite3.connect('Food_Insecurity.sqlite')

conn.execute('''CREATE TABLE food_insecurity (
    Year INT,
    County_FIPS INT,
    "County" VARCHAR,
    "State" VARCHAR,
    "Diagnosed Diabetes Percentage" FLOAT,
    "Food Insecurity" FLOAT
)''')

Food_Insecurity.to_sql("food_insecurity", conn, if_exists='replace', index=False)

query = "SELECT * FROM food_insecurity"
Food_Insecurity = pd.read_sql(query, conn)

conn.commit()
conn.close()

Severe_Housing_Cost_Burden = pd.read_csv('Severe_Housing_Cost_Burden_Percent.csv')

conn = sqlite3.connect('Severe_Housing_Cost_Burden_Percent.sqlite')

conn.execute('''severe_housing_cost_burden (
    Year INT,
    County_FIPS INT,
    "County" VARCHAR,
    "State" VARCHAR,
    "Diagnosed Diabetes Percentage" FLOAT,
    "Severe Housing Cost Burden" FLOAT
)''')

query = "SELECT * FROM severe_housing_cost_burden"
Severe_Housing_Cost_Burden = pd.read_sql(query, conn)

conn.commit()
conn.close()