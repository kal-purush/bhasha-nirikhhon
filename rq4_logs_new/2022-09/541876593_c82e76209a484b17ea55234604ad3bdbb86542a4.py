import re
import json
import syllapy
import syllables
import pandas as pd
import numpy as np

from tqdm import tqdm
from nltk.corpus import cmudict


class SimpsonsHaiku():

    def __init__(self, haiku_df=None):
        self.file_path = 'dataset/simpsons_script_lines.csv'
        self.strip_list = [',', '.', '?', '!', ':', '\\', '"']
        self.nltk_dict = cmudict.dict()
        self.syllapy_dict = syllapy.WORD_DICT
        self.simpson_dict = json.load(open('simpson_lect.json'))
        self.script = self.load_script()
        self.haiku_df = haiku_df

    def load_script(self, 
                    error_bad_lines=False,
                    speaking_only=True):
        """Load Simpsons script into pandas DataFrame."""

        tqdm.pandas()
        script_lines = pd.read_csv(self.file_path, error_bad_lines=error_bad_lines,
                                dtype={'speaking_lines' : bool})
        
        # Data cleaning on speaking lines
        script_lines['speaking_line'] = script_lines['speaking_line'].replace({'true': True, 'false': False})

        if speaking_only:
            script_lines = script_lines[script_lines['speaking_line'] == True]
            script_lines = script_lines[~script_lines['normalized_text'].isna()]

        # Cleaning missing values
        script_lines[['location_id', 'character_id']] = script_lines[['location_id', 'character_id']].fillna(-1)
        script_lines['raw_character_text'] = script_lines['raw_character_text'].fillna('Missing Character')
        script_lines['raw_location_text'] = script_lines['raw_location_text'].fillna('Missing Location')

        # Split longer lines of dialogue based on delimiters and explode to longer format
        script_lines['spoken_words_split'] = script_lines['spoken_words'].str.replace('?', '.').str.replace('!', '.').str.replace('/', '.').str.split('.')  # split on '.!?/', but could extend to ':;,'
        # TODO Regex version instead? Keep (multiple) elimiters? Exclude Mr. etc.
        # script_lines['spoken_words_split'] = script_lines['spoken_words'].apply(lambda x: re.split('[?!/]', x))  # split on '.!?/', but could extend to ':;,'

        script_lines['number_in_line'] = script_lines['spoken_words_split'].apply(lambda x: [i for i in range(1, len(x)+1)])
        script_lines = script_lines.explode(['spoken_words_split', 'number_in_line'])

        # Order by episode then line sequence
        script_lines = script_lines.sort_values(['episode_id', 'number', 'number_in_line'])

        script_lines['n_syllables'] = script_lines.spoken_words_split.progress_apply(self.count_syllables_line)
        script_lines = script_lines[script_lines.n_syllables > 0]

        # non-explode version
        # script_lines['spoken_words_split'] = script_lines['spoken_words']
        # script_lines = script_lines.sort_values(['episode_id', 'number'])

        return script_lines


    def num_syllables(self, word):
        """Number of syllables using NLTK. Props to user hoju (!) at `https://stackoverflow.com/a/4103234`."""
        
        for char in self.strip_list:
            word = word.replace(char, '')
        word = word.lower()
     
        if word in self.simpson_dict:
            n_syl = self.simpson_dict[word]
        elif word in self.nltk_dict.keys():
            n_syl = [len(list(y for y in x if y[-1].isdigit())) for x in self.nltk_dict[word]][0]
        elif word[-1] == 's' and word[:-1] in self.nltk_dict.keys():
            n_syl = [len(list(y for y in x if y[-1].isdigit())) for x in self.nltk_dict[word[:-1]]][0]
        elif word in self.syllapy_dict:
            n_syl = self.syllapy_dict[word]
        else:  # Resort to estimation
            n_syl = syllables.estimate(word)

        return max(1, n_syl)


    def count_syllables_line(self, line, return_list=False): #, s_dict):
        """"Count number of syllables in a line. Return either the final count or a list of cumulative counts from 
        constituent words.
        """

        words = line.lower().replace('-', ' ').replace('/', ' ').split(' ')
        count = 0

        count_list = []
        for word in words:
            for char in self.strip_list:
                word = word.replace(char, '')
            if word:
                n_syllables = self.num_syllables(word)
            else:
                n_syllables = 0 
            count += n_syllables
            count_list.append(count)
        
        if return_list:
            return count_list
        else:
            return count


    def generate_haiku_df(self, save=False):
        """Generate DataFrame of haikus from corpus."""
        
        haiku_list = []
        for i in tqdm(range(len(self.script))):
            for j in range(i+1, min(i+17, len(self.script))):  # Brute force-ish, could be optimized
                if self.script.n_syllables.iloc[i:j].sum() == 17:
                    aggregated_df = self.script.iloc[i:j].groupby('episode_id').agg(list)  # Select 17-syllable sequences from same episode
                    if aggregated_df.shape[0] == 1:
                        aggregated_df.spoken_words_split = ' '.join(aggregated_df.spoken_words_split.values[0])
                        haiku_list.append(aggregated_df)

        haiku_df = pd.concat(haiku_list)

        # Check for parsability, requiring no word-breaks to confirm to 5-7-5 structure
        haiku_df = haiku_df[haiku_df.spoken_words_split.progress_apply(self.is_parsable_as_haiku)]
        
        self.haiku_df = haiku_df

        if save:
            haiku_df.to_csv('haiku_df.csv')

        return haiku_df


    def is_parsable_as_haiku(self, word_string):
        """Return True if string can be parsed as a haiku."""
        count_list = self.count_syllables_line(word_string, return_list=True)
        
        return (count_list[-1]==17) and (5 in count_list) and (12 in count_list)


    def generate_haiku(self, return_list=False):
        """Using an either an exisiting haiku DataFrame or generating one, sample 
        a haiku, parsed in the 5-7-5 line format. Either return as a list or as a
        string delimited with newline characters.
        """

        if self.haiku_df:
            haiku_df = self.haiku_df
        else:
            haiku_df = self.generate_haiku_df(save=True)

        haiku = haiku_df.sample().spoken_words_split.values[0]
        words = haiku[0].replace('-', ' ').replace('/', ' ').split(' ')
        
        count = 0

        haiku_list = ['', '', '']
        syllable_list = [5, 12, 17]

        i = 0
        for word in words:
            for char in self.strip_list:
                word = word.replace(char, '')
            if word:
                n_syllables = self.num_syllables(word)
            else:
                n_syllables = 0 
            count += n_syllables
            
            if count > syllable_list[i]:
                i+=1
            
            haiku_list[i] += word + ' '
        
        haiku_list = [line.strip() for line in haiku_list]

        if return_list:    
            return haiku_list
        else:
            return '\n'.join(haiku_list)


    # def generate_dictionary(self, script_lines, save=True):
    #     """Generate dictionary of words from Simpsons corpus, together with syllable count."""

    #     tqdm.pandas()
    #     # String all dialogue into one line and split into list of strings
    #     corpus = script_lines['spoken_words'].str.cat(sep=' ')
    #     for char in self.strip_list:
    #         corpus = corpus.replace(char, '')    
    #     corpus_list = corpus.lower().replace('-', ' ').replace('/', ' ').split(' ')

    #     corpus_df = pd.DataFrame({'word' : corpus_list})
    #     simpsons_count = corpus_df.value_counts().reset_index(name='counts')

    #     simpsons_dict = {}
    #     for word in tqdm(simpsons_count['word']):
            
    #         for char in self.strip_list:
    #             word = word.replace(char, '')
    #         word = word.lower()

    #         if word:
    #             n_syllables = self.num_syllables(word)

    #             if word not in simpsons_dict.keys():
    #                 simpsons_dict[word] = n_syllables


    #     self.simpsons_dict = simpsons_dict

    #     if save:
    #         pass  # Do a save of syllable dict to json

    #     # return simpsons_dict


    # def get_haiku_lines(self, save=False):
    #     """Find lines of dialogue that are already self-contained haikus."""
        
    #     script_lines=self.script

    #     tqdm.pandas()
    #     script_lines['syllables'] = script_lines['normalized_text'].progress_apply(self.count_syllables_line)
        
    #     ready_haikus = script_lines[script_lines['syllables'] == 17]

    #     if save:
    #         ready_haikus.to_csv('readymade_haikus.csv')

    #     return ready_haikus


    # def generate_haiku(script_lines):
    #     """Identify existing haikus from script, starting at random point in corpus.
    #     """

    #     line_1, line_2, line_3 = "", "", ""
    #     syllable_count = 0
        
    #     # String all dialogue into one line
    #     generate_dictionary(script_lines)
    #     corpus = script_lines['normalized_text'].str.cat(sep=' ')
    #     corpus_list = corpus.split(' ')

    #     # Select index of starting word at random
    #     random_int = np.random.randint(len(corpus_list))
    #     random_element = corpus_list[random_int] + ' '
    #     line_1 += random_element
    #     syllable_count += syllables.estimate(random_element)

    #     for i in range(random_int + 1, random_int + 17):

    #         element = corpus_list[i] + ' '
    #         element_syllables = syllables.estimate(element)

    #         if syllable_count + element_syllables <= 5:
    #             line_1 += element
    #             # syllable_count += syllables.estimate(element)

    #         elif syllable_count + element_syllables <= 12:
    #             line_2 += element
    #             # syllable_count += syllables.estimate(element)

    #         elif syllable_count + element_syllables <= 17:
    #             line_3 += element

    #         else:
    #             break
                
    #         syllable_count += element_syllables

    #     print(line_1)
    #     print(line_2)
    #     print(line_3)
    #     print(syllable_count)


if __name__=='__main__':
    # print(count_syllables_line('leased your camry from christian brothers auto'))
    # script = load_script()
    # generate_haiku(script)
    # print(count_syllables_line("you're"))
    # print(count_syllables_line("moe youre always moe homer look your house is on tv you"))
    pass