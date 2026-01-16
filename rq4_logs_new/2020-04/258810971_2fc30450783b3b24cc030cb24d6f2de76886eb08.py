import re
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.utils import resample
from sklearn.model_selection import train_test_split

# takes a string and removes punctuation and special characters
# returns the lowercase version of all the valid english words concatenated as one string
def clean_strings(text):
    text = text.lower()
    make_space_chars = re.compile('[/(){}\[\]\|@,;]')
    delete_chars = re.compile('[^0-9a-z #+_]')
    text = make_space_chars.sub(' ', text)
    text = delete_chars.sub('', text)

    return text

# concatenates two lists of strings elementwise.
# Returns a list of strings
def concat_string_lists(list1, list2):
    new_list = []
    for i in range(len(list1)):
        new_list.append(list1[i] + " " + list2[i])
    return new_list


def print_class_percents(positive, neutral, negative, choice, scale_type):
    num_pos = len(positive)
    num_neutral = len(neutral)
    num_neg = len(negative)
    num_total = num_pos+num_neutral+num_neg

    #percent of data that are positive
    print('class percentages: ')
    pos = num_pos/num_total
    print('percent positive: ' + str(pos))
    #percent of data that are neutral
    neut = num_neutral/num_total
    print('percent neutral: ' + str(neut))
    #percent of data that are negative
    neg = num_neg/num_total
    print('percent negative: ' + str(neg))
    print('total number of training rows: ' + str(num_total))
    x = ['positive', 'neutral', 'negative']
    y = [pos*100, neut*100, neg*100]
    plt.bar(x, y)
    plt.ylabel('Percent of Data', fontsize=18)
    ax = plt.gca()
    plt.ylim((0, 100))
    plt.setp(ax.get_xticklabels(), fontsize=18)
    plt.setp(ax.get_yticklabels(), fontsize=18)
   # plt.savefig('class_distribution_' + choice + '_' + scale_type + '.pdf', bbox_inches='tight')
   # plt.show()


def fix_skew(dtrain_x, dtrain_y, resampler, scale_type):
    # combine them back for resampling
    train_data = pd.concat([dtrain_x, dtrain_y], axis=1)

    # separate minority and majority classes
    negative = train_data[train_data.overall < 3]
    positive = train_data[train_data.overall > 3]
    neutral = train_data[train_data.overall == 3]

    print_class_percents(positive, neutral, negative, 'before', scale_type)

    positive, neutral, negative = resampler(positive, neutral, negative)

    print_class_percents(positive, neutral, negative, 'after', scale_type)

    # combine majority and upsampled minority
    fixed_data = pd.concat([negative, neutral, positive])

    fixed_data = fixed_data.sample(frac=1, random_state=23)

    dtrain_x = fixed_data['summary_and_review']
    dtrain_y = fixed_data['overall']

    return dtrain_x, dtrain_y


def upsample(positive, neutral, negative):
    # upsample minority
    neutral_upsampled = resample(neutral,
                                 replace=True,  # sample with replacement
                                 n_samples=len(positive),  # match number in majority class
                                 random_state=27)  # reproducible results
    negative_upsampled = resample(negative,
                                  replace=True,  # sample with replacement
                                  n_samples=len(positive),  # match number in majority class
                                  random_state=27)  # reproducible results

    return positive, neutral_upsampled, negative_upsampled


def downsample(positive, neutral, negative):
    negative_downsampled = resample(negative,
                                    replace=True,  # sample with replacement
                                    n_samples=len(neutral),  # match number in smallest class
                                    random_state=27)  # reproducible results
    positive_downsampled = resample(positive,
                                    replace=True,  # sample with replacement
                                    n_samples=len(neutral),  # match number in smalllest class
                                    random_state=27)  # reproducible results

    return positive_downsampled, neutral, negative_downsampled


# does various things to the dataframe to prepare it for the models
def process_data():
    df = pd.read_json('../reviews.json', lines=True)
    # df = pd.read_pickle ('saved_df.pkl')

    # drop irrelevant columns
    dont_includes = ['reviewTime', 'reviewerID', 'asin', 'style', 'reviewerName', 'unixReviewTime', 'vote', 'image',
                     'verified']
    df = df.drop(dont_includes, axis=1)

    # drop all duplicate rows
    before = df.shape[0]

    df = df.drop_duplicates("summary")
    df = df.drop_duplicates("reviewText")
    after = df.shape[0]

    print("removed " + str(before - after) + " duplicate rows from dataframe")

    # remove rows with no summary
    bool_list1 = (pd.DataFrame(df['reviewText']).applymap(type) == type("a string"))['reviewText']
    bool_list2 = (pd.DataFrame(df['summary']).applymap(type) == type("a string"))['summary']
    before = after
    df = df.loc[bool_list1]
    df = df.loc[bool_list2]
    after = df.shape[0]
    print("removed " + str(before - after) + " rows with non-string summary and reviewText from dataframe")

    # add columns for the length of the summaries and reviews
    summary_length = df['summary'].apply(lambda x: len(x.split()))
    review_length = df['reviewText'].apply(lambda x: len(x.split()))

    df.insert(0, "summary_length", summary_length)
    df.insert(0, "review_length", review_length)

    # concatenate the summary and reviews into one new column
    df.insert(0, 'summary_and_review', concat_string_lists(df['reviewText'].values, df['summary'].values))
    df = df.drop(['reviewText', 'summary'], axis=1)

    # print the total number of words in the dataframe
    print('total number of words: ' + str(df['summary_and_review'].apply(lambda x: len(x.split(' '))).sum()))

    # remove all special characters and make all the text lowercase
    df['summary_and_review'] = df['summary_and_review'].apply(clean_strings)

    #     fixed_reviewso, fixed_overallo = fix_skew(df['summary_and_review'], df["overall"], upsample, 'upsampled')
    #     fixed_reviewsu, fixed_overallu = fix_skew(df['summary_and_review'], df["overall"], downsample, 'downsampled')

    #     print(type(fixed_reviewso))
    #     print(fixed_reviewso.shape)
    #     print(fixed_overallo.shape)
    #     print(fixed_reviewsu.shape)
    #     print(fixed_overallu.shape)

    dtrain_x, dtest_x = train_test_split(df['summary_and_review'], random_state=1)
    dtrain_y, dtest_y = train_test_split(df["overall"], random_state=1)

    # correct skew by over-smpling or under-sampling
    dtrain_xo, dtrain_yo = fix_skew(dtrain_x, dtrain_y, upsample, 'upsampled')
    dtrain_xu, dtrain_yu = fix_skew(dtrain_x, dtrain_y, downsample, 'downsampled')
    dtest_xu, dtest_yu = fix_skew(dtest_x, dtest_y, downsample, 'downsampled')

    print("now " + str(len(dtrain_xo)) + " rows in over-sampled train data")
    print("now " + str(len(dtrain_xu)) + " rows in under-sampled train data")
    print("now " + str(len(dtest_xu)) + " rows in under-sampled tests data")

    return df, dtrain_xo, dtrain_yo, dtrain_xu, dtrain_yu, dtest_yu, dtest_xu