import numpy as np
import pandas as pd
from neuralprophet import NeuralProphet

# Read in the initial dataset
iea_data = pd.read_csv("DSS_Datasets_GHG_Solar_iea_data.csv")


def predictions():
    # Filter data for solar product in selected countries
    solar_filter = iea_data["Product"] == "Solar"
    country_filter = iea_data["Country"].isin(["Netherlands", "Belgium", "Luxembourg"])
    data = iea_data[solar_filter & country_filter]

    # Convert Time to datetime and sort
    data["Time"] = pd.to_datetime(data["Time"], format="%B %Y")
    data = data.sort_values(by=["Time"], ascending=True)
    data = data.loc[data["Time"] >= "2017-01-01"]

    # Separate data by country
    NLdata = data[data["Country"] == "Netherlands"]
    BEdata = data[data["Country"] == "Belgium"]
    LUXdata = data[data["Country"] == "Luxembourg"]

    # Rename columns
    NLdatamin = NLdata.rename(columns={"Time": "ds", "Value": "y"})
    BEdatamin = BEdata.rename(columns={"Time": "ds", "Value": "y"})
    LUXdatamin = LUXdata.rename(columns={"Time": "ds", "Value": "y"})

    # Combine and sum data
    BENE = (
        pd.concat([NLdatamin, BEdatamin])
        .groupby(["ds"])
        .sum(numeric_only=True)
        .reset_index()
    )
    BENELUXdatamin = (
        pd.concat([BENE, LUXdatamin])
        .groupby(["ds"])
        .sum(numeric_only=True)
        .reset_index()
    )

    # Model fitting
    mBENELUX = NeuralProphet()
    mBENELUX.fit(BENELUXdatamin, freq="D")

    # Predict future data
    df_futureBENELUX = mBENELUX.make_future_dataframe(
        BENELUXdatamin, n_historic_predictions=False, periods=48
    )
    BENELUXforecast = mBENELUX.predict(df_futureBENELUX)

    # Drop the redundant 'y' column and rename 'yhat1'
    BENELUXforecast.drop(columns=["y"], inplace=True, errors="ignore")
    BENELUXforecast.rename(columns={"yhat1": "y"}, inplace=True)

    # Reset indices
    BENELUXdatamin.reset_index(drop=True, inplace=True)
    BENELUXforecast.reset_index(drop=True, inplace=True)

    # Concatenate the original data with the forecast
    BeneluxPred = pd.concat([BENELUXdatamin, BENELUXforecast], ignore_index=True)

    return BeneluxPred


# Call the function and print the result
predictions_df = predictions()
predictions_df.to_csv("/usr/src/app/output/predictions.csv", index=False)