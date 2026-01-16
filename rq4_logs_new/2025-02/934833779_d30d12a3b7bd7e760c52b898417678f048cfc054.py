import sys
from bea_fetch.fetch_bea_data import fetch_bea_nipa_data_to_csv

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python script.py <api_key> <table> <year> <frequency> <output_file>")
        sys.exit(1)
    
    fetch_bea_nipa_data_to_csv(
        api_key=sys.argv[1],
        table=sys.argv[2],
        year=sys.argv[3],
        frequency=sys.argv[4],
        output_file=sys.argv[5]
    ) 
    