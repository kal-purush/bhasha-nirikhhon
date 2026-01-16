import pandas as pd

if __name__ == "__main__":
    ai_df = pd.read_excel(
        "workspace/world bank data/AR-Company-Indicators_2023Q3.xlsx",
        sheet_name="Company PACTA Plug-In - EO",
    )

    # ai_df = ai_df[ai_df["plant_location"] == "MY"]

    petronas_companies = ai_df.loc[
        ai_df["name_company"].str.contains("misc", case=False),
        ["company_id", "name_company"],
    ].drop_duplicates()

    petronas_companies = {
        "petronas carigali sdn. bhd.": "petronas carigali sdn. bhd.",
        "petronas refinery & petrochemical corp. sdn. bhd.": "petronas carigali sdn. bhd.",
    }

    solarvest_companies = {
        "solarvest holdings bhd.": "solarvest holdings bhd.",
        "solarvest sdn. bhd.": "solarvest holdings bhd.",
    }