import pandas as pd

def clean_column_names(df):
    # Clean and standardize column names
    df.rename(columns={"ST": "state"}, inplace=True)
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    return df

def clean_invalid_values(df):
    # Clean inconsistent values
    df["state"] = df["state"].replace({'WA': 'Washington', 'AZ': 'Arizona', 'Cali': 'California'})
    df["gender"] = df["gender"].replace({'Femal': 'F', 'Male': 'M', 'female': 'F'})
    df["education"] = df["education"].replace({'Bachelors': 'Bachelor'})

    df["customer_lifetime_value"] = df["customer_lifetime_value"].str.replace("%", "")
    df["vehicle_class"] = df["vehicle_class"].replace({'Luxury Car': 'Luxury', 'Sports Car': 'Luxury', "Luxury SUV": 'Luxury'})
    return df

def format_data_types(df):
    # Format data types
    df['customer_lifetime_value'] = pd.to_numeric(df['customer_lifetime_value'], errors='coerce')
    df['number_of_open_complaints'] = df['number_of_open_complaints'].apply(
        lambda x: x.split('/')[1] if pd.notna(x) else x)
    df['number_of_open_complaints'] = pd.to_numeric(df['number_of_open_complaints'], errors='coerce')
    return df

def handle_null_values(df):
    # Handle null values
    df.dropna(how="all", inplace=True)
    df["customer_lifetime_value"] = df["customer_lifetime_value"].fillna(df["customer_lifetime_value"].mean())
    df["gender"] = df["gender"].fillna("N")
    num_vars = df.select_dtypes(include=["float64", "int64"]).columns
    df[num_vars] = df[num_vars].apply(pd.to_numeric, downcast='integer')
    return df

def remove_duplicates(df):
    # Remove duplicate rows
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def main_cleaning_function(df):
    # Main function to apply all the cleaning steps
    df = clean_column_names(df)
    df = clean_invalid_values(df)
    df = format_data_types(df)
    df = handle_null_values(df)
    df = remove_duplicates(df)
    return df