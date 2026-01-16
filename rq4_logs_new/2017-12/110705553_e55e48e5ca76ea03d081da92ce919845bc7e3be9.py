import pandas as pd
import numpy as np
import os
from progress.bar import Bar


def convertToCSV(array_data):
    df =  pd.DataFrame(array_data)
    columns = ['loosing_hero_id_1', 'loosing_hero_id_2', 'loosing_hero_id_3', 'loosing_hero_id_4', 'loosing_hero_id_5', 'winning_hero_id_1', 'winning_hero_id_2', 'winning_hero_id_3', 'winning_hero_id_4', 'winning_hero_id_5']
    df.to_csv('picks_data.csv', sep=",", header=columns, index=None)

def main():
    directory = '../../Projects/Github/Jannus_dota2/data/matches/'
    files = []
    for file in os.listdir(directory):
        if file.endswith('.npy'):
            files.append(directory + file)

    data = []
    file_pbar = Bar('File Progress', max = len(files))

    for f in files:
        file_pbar.next()
        file_data = np.load(f)
        temp = np.nonzero(file_data.astype(int))[1]
        data.append(temp)

    convertToCSV(data)

main()