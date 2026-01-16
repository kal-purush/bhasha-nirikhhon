import sys
import pandas as pd
import numpy as np

def clean_data(df):
    
    def is_float(value):
        try:
            float(value)
            return True
        except ValueError:
            return False
    # Handle dimension information
    df = df.loc[~pd.isnull(df.dim_0) & ~pd.isnull(df.dim_1)]
    df = df[df.dim_0.apply(lambda x: is_float(x)) & df.dim_1.apply(lambda x: is_float(x))]
    df.dim_0 = df.dim_0.apply(np.float)
    df.dim_1 = df.dim_1.apply(np.float)
    
    # Handle price information
    df['price_value'] = df.price_sold.apply(lambda x: int(x.split()[1].replace(',', '')))
    df['currency'] = df.price_sold.apply(lambda x: x.split()[0])

    return df

def find_similar_images(df1, df2, dim_0, dim_1, threshold):
    # Calculate absolute differences for both auctions and find similar objects
    abs_diff_dim0_df1 = abs(df1.dim_0 - dim_0)
    abs_diff_dim1_df1 = abs(df1.dim_1 - dim_1)
    
    abs_diff_dim0_df2 = abs(df2.dim_0 - dim_0)
    abs_diff_dim1_df2 = abs(df2.dim_1 - dim_1)
    
    df1_similar = df1.loc[(abs_diff_dim0_df1 <= threshold) & (abs_diff_dim1_df1 <= threshold)]
    df2_similar = df2.loc[(abs_diff_dim0_df2 <= threshold) & (abs_diff_dim1_df2 <= threshold)]
    
    # Calculate average values of artists who appear in both auctions
    avg_value_df1 = df1_similar[['artist', 'price_value']].groupby('artist').mean()
    avg_value_df2 = df2_similar[['artist', 'price_value']].groupby('artist').mean()
    
    artists_avg = avg_value_df1.merge(avg_value_df2, how='inner', on='artist', suffixes=['_1117', '_0318'])
    
    # Retrieve lot info for similar pieces
    df1_list = df1_similar.loc[df1_similar.artist.isin(artists_avg.index),
                              ['artist', 'title', 'dim_0', 'dim_1', 'price_value']].set_index(['artist', 'title'])
    
    df2_list = df2_similar.loc[df2_similar.artist.isin(artists_avg.index),
                              ['artist', 'title', 'dim_0', 'dim_1', 'price_value']].set_index(['artist', 'title'])
    
    return df1_list, df2_list, artists_avg

def main():
    df1 = clean_data(pd.read_csv('data/impressionist-and-modern-art-27255_lot_info.csv', na_values = ['N.A.']))
    df2 = clean_data(pd.read_csv('data/impressionist-and-modern-art-27400_lot_info.csv', na_values = ['N.A.']))
    
    dim_0 = float(sys.argv[1])
    dim_1 = float(sys.argv[2])
    threshold = float(sys.argv[3])
    
    df1_sim, df2_sim, artists_avg = find_similar_images(df1, df2, dim_0, dim_1, threshold)
    
    print("Similar pieces from the first auction: \n {}\n".format(df1_sim.to_string()))
    print("Similar pieces from the second auction: \n {}\n".format(df2_sim.to_string()))
    print("Average values by artist: \n {}".format(artists_avg.to_string()))
    

if __name__ == '__main__':
    main()
    