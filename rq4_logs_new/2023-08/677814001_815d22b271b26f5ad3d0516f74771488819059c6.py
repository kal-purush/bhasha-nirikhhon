# Imports
import os

# Dependencies
import matplotlib

# Locals
from lssParser import *
from lssGraphs import *

# TODO:
# add .csv output with options
# properly account for skipped splits

# prompts the user for a .lss file, if it's valid outputs text files, graphs, and (TODO: csv)
def main():
    matplotlib.use('TkAgg')
    while True:
        try:
            lss_file = input('Enter the path to a valid .lss file (or "q" to quit): ')
            # check if user wants to quit
            if lss_file.lower() == 'q' or lss_file.lower() == 'quit':
                break
            # make sure it's a .lss file
            if not lss_file.lower().endswith('.lss'):
                raise ValueError('Invalid file format. File must be of type .lss')
            else:
                # check first two lines 
                with open(lss_file, 'r') as file:
                    header = [file.readline() for _ in range(2)]
                    if not header[0].startswith('ï»¿<?xml version="1.0" encoding="UTF-8"?>'):
                        raise ValueError('Invalid XML version header')
                    if not header[1].strip().startswith('<Run version='):
                        raise ValueError('Missing <Run> tag.')
                result = open_lss_file(lss_file)
                if result:
                    # get the name of the file & create a folder for output
                    file_name = os.path.splitext(os.path.basename(lss_file))[0]
                    folder_path = os.path.join(os.getcwd(), file_name)
                    os.makedirs(folder_path, exist_ok=True)
                    print('Created directory', folder_path)
                    
                    split_data = read_lss_file(result, folder_path)
                
                    write_split_stats(split_data, folder_path, file_name)
                    write_graphs(split_data, folder_path)
                    
                    #write_csv(split_data, folder_path)
                else:
                    print('Failed to open', lss_file)
                    
        except (FileNotFoundError, ET.ParseError, ValueError) as e:
            print(f'Error: {e}')

# writes data collected and calculated from the .lss file to a .txt file
def write_split_stats(split_data, folder_path, file_name):
    file_path = f'{folder_path}\\{file_name}_output.txt'
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(f'Game Name:      {split_data.game_name}\n')
        file.write(f'Category Name:  {split_data.category_name}\n')
        file.write(f'Layout Path:    {split_data.layout_path}\n')
        file.write(f'Timer Offset:   {split_data.timer_offset}\n')
        file.write(f'Runs Started:   {split_data.runs_started}\n')
        file.write(f'Runs Finished:  {split_data.runs_finished}\n')
        file.write(f'Total Runtime:  {split_data.total_runtime}\n')
        file.write(f'Total Playtime: {split_data.total_playtime}\n')
        file.write('\nSegments\n')
        
        file.write('\nNOTE: averages, medians, and standard deviations are more heavily weighted towards recent runs.\n')
        file.write('\nNOTE: results may be incorrect if splits were rearranged/changed.\n')
        
        for index, current_segment in enumerate(split_data.segments, start=1):
            file.write(f'{index}. {current_segment.name.encode("utf-8").decode("utf-8")}\n')
            file.write(f'    - Split Time (PB):    {current_segment.split_time_pb}\n')
            file.write(f'    - Segment Time (PB):  {current_segment.segment_pb}\n')
            file.write(f'    - Best Time:          {current_segment.segment_gold.time}, on attempt {current_segment.segment_gold.id}, which started on {current_segment.segment_gold.run_date} at {current_segment.segment_gold.run_time}.\n')
            file.write(f'    - Worst Time:         {current_segment.segment_worst.time}, on attempt {current_segment.segment_worst.id}, which started on {current_segment.segment_worst.run_date} at {current_segment.segment_worst.run_time}.\n')
            file.write(f'    - Average Time:       {current_segment.stats.average}\n')
            file.write(f'    - Median Time:        {current_segment.stats.median}\n')
            file.write(f'    - Std Deviation:      {current_segment.stats.stdev}\n')
            file.write(f'    - Possible Time Save: {current_segment.possible_time_save}\n')
            file.write(f'    - This segment is completed {current_segment.stats.finished_rate} of the time.\n')
            file.write(f'    - This segment is above average {current_segment.stats.decent_rate} of the time.\n\n')
            
    print('Successfully output .lss data to', file_path)

# creates and outputs a number of graphs to a file:
# * Line graph: segment duration over time (1 for each segment)
# * Bar graph: standard deviation for each segment
# * Bar graph: percentage of above average segments
# * Bar graph: possible time save in PB
# * Line graph: Run duration over time
def write_graphs(split_data, folder_path):
    segment_names = []
    
    # get list of all segment names
    for current_segment in split_data.segments:
        segment_names.append(current_segment.name)
    
    # dictionary to store all of the segment duration graphs
    get_segment_duration_graphs(split_data, folder_path)
    print('Successfully output segment graphs to', folder_path)

    # list to store the other graphs
    get_graphs(split_data, segment_names, folder_path)
    print('Successfully output other graphs to', folder_path)

if __name__ == '__main__':
    main()