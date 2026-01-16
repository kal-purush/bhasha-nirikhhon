if __name__ == "__main__":
    import sys
    import argparse
    import finlib.core as finlib
    import pdb
    # python getsymb.py TWTR 1m 1week -fformat :6.2f -iformat :10d

    # Parse the input arguments
    parser = argparse.ArgumentParser(description="Print list of S&P500 symbols.")
    parser.add_argument("-n", help="Number of symbols to fetch.", action="store", dest = "n", type=int)    
    args = parser.parse_args()

    n = args.n if args.n else 1000
    
    # Print the symbols
    print "\n".join([r.tickersymbol for r in finlib.get_sp500()[:n]]),
    exit(0)
    
    
    


    
    
    