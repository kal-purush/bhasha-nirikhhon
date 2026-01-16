import sys,os

# Add timber calc to path and import it as a module
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import timber_calc as tc

# Set local variables
input_path = "/home/ygritte/github/Timber-Calculations/sample_data/ExampleA-TreeSpeciesDBH.csv"
output_path = "/home/ygritte/github/Timber-Calculations/output.csv"

# Import data
data = tc.import_data(input_path,",")

# Run unit test
data = tc.vol_international(data,"dbh",16)

# Export data
tc.export_data(output_path, data)