from sqlalchemy import create_engine, MetaData, Table
import json
import pandas as pd
import scipy.stats as stats
import matplotlib
matplotlib.use('tkagg')
import matplotlib.pyplot as plt
import scipy.stats as stats
import statsmodels.api as sm
import statsmodels.formula.api as ols
from pingouin import pairwise_tukey
import pingouin as pg
import copy
import seaborn as sns
import numpy as np

'''
CONSTANTS and FLAGS
'''
FILENAME_OUTPUTS = "outputs/"
FILENAME_PLOTS = FILENAME_OUTPUTS + "plots/"
FILENAME_ANOVAS = FILENAME_OUTPUTS + "anovas/"
FILENAME_PREFIX = ""

FLAG_EXPORT = False


# Types of analysis
A_CORRECT_END = 'correct-end'

A_PCT_UNSURE = 'pct_unsure'
A_PCT_CORRECT = 'pct_correct'
A_PCT_INCORRECT = 'pct_incorrect'

A_REVERSALS = 'reversals'

A_ENV_START_THRESHOLD   = 'envelope_start_threshold'
A_ENV_START_ACC         = 'envelope_start_accuracy'
A_ENV_START_CERT        = 'envelope_start_certainty'

A_ENV_END_THRESHOLD = 'envelope_end_threshold'
A_ENV_END_ACC       = 'envelope_end_accuracy'
A_ENV_END_CERT      = 'envelope_end_certainty'

A_ENV_LEN_THRESHOLD = 'envelope_length_threshold'
A_ENV_LEN_ACC       = 'envelope_length_accuracy'
A_ENV_LEN_CERT      = 'envelope_length_certainty'


A_TT_CUTOFF = 'tt_cutoff'
A_TT_ACC = 'tt_accuracy'
A_TT_CERT = 'tt_certainty'

A_FLIPPED = 'is-flipped'

P_GLITCHES = 'glitches'
P_POST_EVENTS = 'post-events'
P_LOOKUP = 'lookup-packet'
P_QUAL_CHECK = 'qual_check_correct_end'

OUTPUT_GRAPH_BOXPLOT = True
OUTPUT_GRAPH_STRIPPLOT = True
OUTPUT_GRAPH_BLENDED = True
OUTPUT_CALC_ANOVA = True

STATUS_GLITCH_UNSUPPORTED_BROWSER = "unsupported browser"
STATUS_GLITCH_NO_EVENTS = "no events found"
STATUS_GLITCH_EVENT_PAST_VIDEO_END = "past video end"
STATUS_NORMAL = "glitch-free"

LABELS_PATHING = {}
LABELS_PATHING['Omn'] = "Omniscient"
LABELS_PATHING['M'] = "Multi"
LABELS_PATHING['SA'] = "Single:A\n (for back-to-robot)"
LABELS_PATHING['SB'] = "Single:B\n (for facing-robot)"

POLARITY_UNSURE      = 0
POLARITY_CORRECT     = 1
POLARITY_INCORRECT   = -1

STATE_UNSURE    = POLARITY_UNSURE
STATE_CORRECT   = POLARITY_CORRECT
STATE_INCORRECT = POLARITY_INCORRECT

# Static math
unsure_top = VALUE_MIDDLE + UNSURE_WINDOW
unsure_bottom = VALUE_MIDDLE - UNSURE_WINDOW

perspectives_file = ['PA','PB']
pathings_file = ['Omn', 'SA', 'SB', 'Multi']

df = pd.read_csv('finalsurveyresults.csv')
print("Pandas data imported from CSV")


#Get the set of uniqueids (no duplicates)
idSet = set()
for ind in df.index: #how to iterate through rows
    row = df.loc[ind]
    idSet.add(row['uniqueid'])
    
print("Number of participants: " + str(len(idSet)))
print("participants: "+ str(idSet))

ageList = []
for ind in df_postquestionnaire.index: #how to iterate through rows
   row = df_postquestionnaire.loc[ind]
   ageList.append(row['age'])

print("Ages: ")
for item in ageList:
   print(item)

genderList = []
for ind in df_postquestionnaire.index: #how to iterate through rows
   row = df_postquestionnaire.loc[ind]
   genderList.append(row['gender'])

print("Genders: ")
for item in genderList:
   print(item)


def analyze_all_participants(df):
    print(df.shape)
    
    df.loc[:, df.dtypes == 'float64']   = df.loc[:, df.dtypes == 'float64'].astype('float')
    df.loc[:, df.dtypes == 'int64']     = df.loc[:, df.dtypes == 'int64'].astype('int')

    print("Time to make some graphs \n")
    return df


def make_anova(df, analysis_label, fn, title):
    SIGNIFICANCE_CUTOFF = .4
    if OUTPUT_CALC_ANOVA:
        anova_text = title + "\n"
        # print("ANOVA FOR ")
        # print(analysis_label)
        # print(df[analysis_label])

        subject_id = 'uniqueid'

        df_col = df[analysis_label]
        val_min = df_col.get(df_col.idxmin())
        val_max = df_col.get(df_col.idxmax())
        homogenous_data = (val_min == val_max)

        if not homogenous_data:
            print("~~~ ANALYSIS FOR " + analysis_label + " ~~~")
            aov = pg.mixed_anova(dv=analysis_label, between=COL_CHAIR, within=COL_PATHING, subject=subject_id, data=df)
            aov.round(3)

            anova_text = anova_text + str(aov)
            aov.to_csv(FILENAME_ANOVAS + fn + 'anova.csv')

            p_vals = aov['p-unc']
            p_chair = p_vals[0]
            p_path_method = p_vals[1]

            if p_chair < SIGNIFICANCE_CUTOFF:
                print("Chair position is significant for " + analysis_label + ": " + str(p_chair))
                # print(title)
            if p_path_method < SIGNIFICANCE_CUTOFF:
                print("Pathing method is significant for " + analysis_label + ": " + str(p_path_method))
                # print(title)

            anova_text = anova_text + "\n"
            # Verify that subjects is legit
            # print(df[subject_id])

            posthocs = pg.pairwise_ttests(dv=analysis_label, within=COL_PATHING, between=COL_CHAIR,
                                      subject=subject_id, data=df)
            # pg.print_table(posthocs)
            anova_text = anova_text + "\n" + str(posthocs)
            posthocs.to_csv(FILENAME_ANOVAS + fn + 'posthocs.csv')
            print()

        else:
            print("! Issue creating ANOVA for " + analysis_label)
            print("Verify that there are at least a few non-identical values recorded")
            anova_text = anova_text + "Column homogenous with value " + str(val_min)


        f = open(FILENAME_ANOVAS + fn + "anova.txt", "w")
        f.write(anova_text)
        f.close()

def make_boxplot(df, analysis, fn, title):
    if OUTPUT_GRAPH_BOXPLOT:
        graph_type = "boxplot"
        plt.figure()
        # plt.tight_layout()
        # title = al_title[analysis] + "\n" + al_y_range
        bx = sns.boxplot(data=df, x=COL_PATHING, y=analysis, hue=COL_CHAIR, order=cat_order)
        # print("San check on data")
        # print(df[analysis])
        # print(df[analysis].columns)

        bx.set(xlabel='Pathing Method')
        ylims = al_y_range[analysis]
        bx.set(ylim=ylims)
        bx.set(title=title, ylabel=al_y_units[analysis])
        figure = bx.get_figure()    
        figure.savefig(FILENAME_PLOTS + fn + graph_type + '.png', bbox_inches='tight')
        plt.close()

def make_stripplot(df, analysis, fn, title):
    if OUTPUT_GRAPH_STRIPPLOT:
            graph_type = "stripplot"
            plt.figure()
            # plt.tight_layout()
            bplot=sns.stripplot(y=analysis, x=COL_PATHING, 
                           data=df_goal, 
                           jitter=True, 
                           marker='o', 
                           alpha=0.8,
                           hue=COL_CHAIR, order=cat_order)
            bplot.set(xlabel='Pathing Method')
            bplot.set(ylim=al_y_range[analysis])
            bplot.set(title=al_title[analysis], ylabel=al_y_units[analysis])
            figure = bplot.get_figure()    
            figure.savefig(FILENAME_PLOTS + fn + graph_type + '.png', bbox_inches='tight')
            plt.close()

def inspect_troublemakers(df_analyzed, list_of_lookup_packets):
    # print(df_analyzed[P_LOOKUP])

    for t in list_of_lookup_packets:
        df_trouble = df_analyzed[df_analyzed[P_LOOKUP] == t]
        df_trouble = df_trouble.iloc[0]

        # Should only be one
        t_id = t
        t_id = t_id.replace("(", "")
        t_id = t_id.replace(")", "")
        t_id = t_id.replace("\'", "")
        t_id = t_id.replace(", ", "-")
        print("Found and generating raw slider log for " + t_id)

        fn = "trouble-" + t_id + ".png"
        plot_analysis_one_participant(df_trouble, t, fn)
        # Add dirty data marker, too


'''
 END: METHOD DECLARATIONS
'''


print("FINISHED")




