import pandas as pd
import yaml

def sample() -> None:
    """
    This function reads a YAML configuration file to extract input and output directory paths, filename, and a fraction
    of rows to be sampled from a CSV file. It performs random sampling on the CSV file using the specified fraction of rows
    and saves the sampled rows in a new CSV file. The function does not return anything but prints progress and completion messages.

    Args:
    - None

    Returns:
    - None
    """
    
    # Open the YAML configuration file and load configuration settings
    with open("config.yml", encoding='utf-8', mode='r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.Loader)
        read_path = cfg['sample']['read_path'] # Input directory path
        fname = cfg['sample']['filename'] # Input filename
        output_path = cfg['sample']['output_path'] # Output directory path
        sample_frac = cfg['sample']['sample_frac'] # Fraction of rows to be sampled from CSV file
        
    # Assert that the sample fraction is within the range (0, 1)
    assert 0 < sample_frac < 1, f"'sample_frac' is out of range. Parameter value received is {sample_frac}. fraction must be within (0,1)."

    # Print a message indicating that random sampling is about to commence
    print(f"Commencing random sampling using a fraction of {sample_frac}")

    # Read the CSV file into a pandas dataframe and sample a fraction of rows
    df_raw = pd.read_csv(f'{read_path}/{fname}', engine='pyarrow')
    df_sample = df_raw.sample(frac=sample_frac)

    # Save the sampled rows in a new CSV file
    df_sample.to_csv(f'{output_path}/dblp_sample.csv')

    # Print a message indicating that random sampling has completed and the number of rows that have been sampled
    print(f'Random sampling completed. {len(df_sample)} rows have been sampled into a new file: dblp_sample.csv')


if __name__ == "__main__":
    sample()



