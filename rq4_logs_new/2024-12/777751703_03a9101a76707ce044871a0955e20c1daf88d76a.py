import ast
import sys
sys.path.append('./core/')
from miniML import MiniTrace, EventDetection
import tensorflow as tf
print(tf.__version__)
import pandas as pd

# MINI data

OUT_DIR = '/alzheimer/verjinia/data/analysed_minis/'

data_path = '/alzheimer/verjinia/data/recordings/minis_events/'
mini_df = pd.read_excel('/alzheimer/verjinia/data/metadata_tables/' + \
    '2024-12-02minis_meta.xlsx')

# analysed_df = pd.read_excel(OUT_DIR + 'all_analyzed_events.xlsx')
# start_indx = len(analysed_df)
start_indx = 0
num_swps_to_analyze = 4 # takes the first 4 decent sweeps

scaling = 1
unit = 'pA'
win_size = 750
stride = int(win_size/30)

result_df = pd.DataFrame(columns = ['OP','patcher','file_name', 'channel', 'slice', \
                                    'treatment', 'hrs_incubation', 'analysis_len (sec)', \
                                    'event count', 'amplitude mean', 'amplitude std', \
                                    'amplitude median', 'charge mean', 'risetime mean (10 - 90)', \
                                    'halfdecaytime mean', 'frequency (Hz)', 'comment'])

for i, fn in enumerate(mini_df['filename'][start_indx:]):
    i = i + start_indx

    if i == 280: # skipped files, can't solve problem
        continue
    
    file_path  = data_path + fn
    chan = mini_df['cell_ch'].values[i] 
    swps_keep = ast.literal_eval(mini_df.swps_to_analyse.values[i])

    save_individ_dir = OUT_DIR + 'indivd_model_retrain1_Oct_minis/' +f"{fn[:-4]}_ch{str(chan)}.csv"

    if num_swps_to_analyze <= len(swps_keep):
        swps_keep = ast.literal_eval(mini_df.swps_to_analyse.values[i])[:num_swps_to_analyze]
    swps_delete = list(set(range(30)) - set(swps_keep))

    trace = MiniTrace.from_axon_file_MP(filepath = file_path,
                                        channel = chan,
                                        scaling = scaling,
                                        unit = unit,
                                        sweeps_delete = swps_delete,
                                        first_point = 0,
                                        last_point = 5500)
    

    detection = EventDetection(data = trace,
                            model_path='./models/transfer_learning/human_pyramids_L2_3/2024_Oct_29_lstm_transfer.h5',
                            window_size = win_size,
                            model_threshold = 0.5,
                            batch_size = 512,
                            event_direction = 'negative',
                            training_direction = 'negative',
                            compile_model = True)

    detection.detect_events(stride=stride,
                            eval = True,
                            convolve_win = 20,
                            resample_to_600 = True)


    df_meta = pd.DataFrame({'OP': mini_df.OP.values[i],
                            'patcher': mini_df.patcher.values[i], 
                            'file_name' : fn,
                            'channel' : str(chan), 
                            'slice' : mini_df.slice.values[i],
                            'treatment': mini_df.treatment.values[i], 
                            'hrs_incubation': mini_df.hrs_incubation.values[i],
                            'analysis_len (sec)': len(trace.data / 20_000)}, index = [0])
    

    individual, avgs = detection.save_to_csv(save_individ_dir)

    df_to_add = pd.concat([df_meta, avgs.T], axis = 1)
    result_df = pd.concat([result_df, df_to_add], axis = 0)

    if (i + 1) % 10 == 0: 
        result_df.to_excel(OUT_DIR + 'all_analyzed_events.xlsx', index=False)
 

 # quick plot


plt.figure(figsize=(10, 6))

# Plot the scatter points with jitter using np.linspace
for k, treatment in enumerate(df['treatment'].unique()):
    treatment_data = df[df['treatment'] == treatment]
    mean =treatment_data['amplitude mean'].mean()
    jitter = np.linspace(-0.1, 0.1, len(treatment_data)) 
    plt.scatter(k + jitter, treatment_data['amplitude mean'], label=f'Treatment {treatment}')
    plt.scatter(k, mean, color='black', s=100, edgecolor='black', linewidth=1)

# Customize the plot
plt.xlabel('Day')
plt.ylabel('Amplitude Mean')
plt.title('Repatch Mean Amplitudt')
plt.legend()
plt.grid(True)
plt.show()

# Print the values of the means by day and treatment
treatment_means = df.groupby(['treatment'])['amplitude mean'].mean()
print("Means by Day and Treatment:")
print(treatment_means)

