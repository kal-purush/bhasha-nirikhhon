from __future__ import division

from copy import deepcopy
import random
import sys

args = sys.argv

if len(args) != 2:
    sys.exit('Usage: python cross_validate.py train_file')

train_file = args[1]

start_tag, end_tag = '<s>', '</s>'

pos_tags_list = [start_tag]
pos_tags_dict = {start_tag: 0}
num_tags = 1

mat_transitions, map_emissions = [], {}
num_transitions, num_emissions = {}, {}
sum_transitions, sum_emissions = {}, {}

a = mat_transitions

# 4. Viterbi function which accepts a list of tokens and a smoothed emission map and returns a list of POS tags (by index)
def viterbi(tokens, b):
    global a

    # a:
    #   N*N matrix where N = num_tags (includes <s> and </s>, already +2)
    #   P(curr_tag|prev_tag)
    #   a[prev_tag][curr_tag]

    # b:
    #   len(N) map where N = num_tags (includes <s> and </s>, already +2)
    #   P(curr_token|curr_tag)
    #   if b[curr_tag][curr_token] then b[curr_tag][curr_token] else 0

    T = len(tokens)

    mat_viterbi = [[0 for token in range(T)] for tag in range(num_tags)]
    mat_backptr = [[0 for token in range(T)] for tag in range(num_tags)]

    # 4a. Initialize viterbi matrix with <s> -> tag values from transition matrix
    for s in range(num_tags):
        token, tag = tokens[0], pos_tags_list[s]
        if token in b[tag]:
            mat_viterbi[s][0] = a[0][s] * Decimal(b[tag][token])

    # 4b. Iteratively populate the viterbi matrix
    for t in range(1, T):
        for s in range(num_tags):
            v_max_prob, b_max_prob, b_max_s = 0, 0, 0
            # 4bi. Get argmax s' (POS tag) based on the viterbi probabilities in the previous t (token)
            for s_prime in range(num_tags):
                b_curr_prob = mat_viterbi[s_prime][t-1] * a[s_prime][s]
                v_curr_prob = 0
                token, tag = tokens[t], pos_tags_list[s]
                if token in b[tag]:
                    v_curr_prob = b_curr_prob * Decimal(b[tag][token])
                if v_curr_prob > v_max_prob:
                    v_max_prob = v_curr_prob
                if b_curr_prob > b_max_prob:
                    b_max_prob = b_curr_prob
                    b_max_s = s_prime
            mat_viterbi[s][t] = v_max_prob
            mat_backptr[s][t-1] = b_max_s

    # 4c. Populate final column of viterbi matrix with tag -> </s> values from transition matrix
    v_max_prob, b_max_prob, b_max_s = 0, 0, 0
    for s_prime in range(num_tags):
        b_curr_prob = mat_viterbi[s_prime][-1] * a[s_prime][-1]
        v_curr_prob = 0
        token, tag = tokens[-1], pos_tags_list[s_prime]
        if token in b[tag]:
            v_curr_prob = b_curr_prob * Decimal(b[tag][token])
        if v_curr_prob > v_max_prob:
            v_max_prob = v_curr_prob
        if b_curr_prob > b_max_prob:
            b_max_prob = b_curr_prob
            b_max_s = s_prime
    mat_viterbi[-1][-1] = v_max_prob
    mat_backptr[-1][-1] = b_max_s

    # 4d. Walk through the backpointer matrix to get a path of POS tags (in reverse)
    max_s = -1
    fin_back_path = []
    for t in range(T-1, -1, -1):
        max_s = mat_backptr[max_s][t]
        fin_back_path.append(max_s)

    # 4e. Return the path of POS tags in sequence from first to last token
    return fin_back_path[::-1]

# 5. Function to smooth the emission map with a somewhat biased Witten Bell smoothing
def get_updated_emission_map(tokens, b):
    unseen = []
    # 5a. Add unseen test tokens to the unseen list
    for token in tokens:
        seen = False
        for tag in b:
            if token in b[tag]:
                seen = True
                break
        if not seen:
            unseen.append(token)
    # 5b. Smooth if there are unseen test tokens
    if unseen:
        num_unseen = len(unseen)
        vocabulary = set(tokens)
        for tag in b:
            vocabulary.update(b[tag].keys())
        Z = num_unseen * num_tags
        T = len(vocabulary) - Z
        for tag in b:
            tag_freq = len(b[tag])
            b[tag] = {token: ((b[tag][token] * tag_freq) / (tag_freq + T)) for token in b[tag]}
            for unseen_token in unseen:
                b[tag][unseen_token] = T / (Z * (tag_freq + T))
    return b

# 1. Create data structures to map from index to POS tag (list) and from POS tag to index (dict)
# This is to make sense of each index in the 2D transition matrix
with open('pos.key', 'r') as k:
    for tag in k:
        stripped_tag = tag.strip()
        pos_tags_list.append(stripped_tag)
        pos_tags_dict[stripped_tag] = num_tags
        num_tags += 1
    pos_tags_list.append(end_tag)
    pos_tags_dict[end_tag] = num_tags
    num_tags += 1

tag_count_dict = {tag: 0 for tag in pos_tags_list}
tag_token_dict = {tag: {} for tag in pos_tags_list}
tag_tag_dict = {tag: {tag: 0 for tag in pos_tags_list} for tag in tag_count_dict}

list_training_set = []
num_training_rows = 0
curr_validate_indices = set()

# 2. Open the training data file and parse the tokens and tags
# Count tag -> tag for transition matrix and tag -> word for emission map
with open(train_file, 'r') as f:
    for line in f:
        prev_token, prev_tag = '', start_tag
        split_line = line.strip().split(' ')
        list_training_set.append(split_line)

k = 10
num_training_rows = len(list_training_set)
val_score = 0
val_count = k * (num_training_rows // k)

for _ in range(k):
    mat_transitions = [[0 for tag in range(num_tags)] for tag in range(num_tags)]
    num_transitions = deepcopy(tag_tag_dict)
    map_emissions = deepcopy(tag_token_dict)
    num_emissions = deepcopy(tag_token_dict)
    sum_transitions = deepcopy(tag_count_dict)
    sum_emissions = deepcopy(tag_count_dict)

    curr_validate_indices = set(random.sample(range(num_training_rows), num_training_rows // k))
    curr_validate_list = []

    for i in range(num_training_rows):
        if i in curr_validate_indices:
            curr_validate_list.append(list_training_set[i])
        else:
            prev_token, prev_tag = '', start_tag
            split_line = list_training_set[i]
            for tagged_token in split_line:
                split_token = tagged_token.split('/')
                curr_token = ''.join(split_token[:-1])
                curr_tag = split_token[-1]
                num_transitions[prev_tag][curr_tag] += 1
                sum_transitions[prev_tag] += 1

                if curr_token not in num_emissions[curr_tag]:
                    num_emissions[curr_tag][curr_token] = 1
                    sum_emissions[curr_tag] += 1
                else:
                    num_emissions[curr_tag][curr_token] += 1
                    sum_emissions[curr_tag] += 1

                prev_token, prev_tag = curr_token, curr_tag

            curr_tag = '</s>'
            num_transitions[prev_tag][curr_tag] += 1
            sum_transitions[prev_tag] += 1

            # 4. Translate transition counts (on a dictionary) to transition matrix (2D list)
            # Divide the frequency of tagA -> tagB by the total frequency of tagA
            for prev_tag in num_transitions:
                count = sum_transitions[prev_tag]
                if count > 0:
                    row = pos_tags_dict[prev_tag]
                    for next_tag in num_transitions[prev_tag]:
                        col = pos_tags_dict[next_tag]
                        mat_transitions[row][col] = num_transitions[prev_tag][next_tag] / sum_transitions[prev_tag]

            # 5. Iterate through the emission map and divide the frequency of tag -> word by tag
            for curr_tag, curr_emission in num_emissions.items():
                map_emissions[curr_tag] = {k: (v / sum_emissions[curr_tag]) for k, v in curr_emission.items()}

    # 6. For each test case (line), re-smooth the original emission map and pass it to the viterbi function
    for line in curr_validate_list:
        tokens, ans_tags = [], []
        split_line = line
        for tagged_token in split_line:
            split_token = tagged_token.split('/')
            curr_token = ''.join(split_token[:-1])
            curr_tag = split_token[-1]
            tokens.append(curr_token)
            ans_tags.append(curr_tag)
        a = mat_transitions
        val_tags = list(map(lambda index: pos_tags_list[index], viterbi(tokens, get_updated_emission_map(tokens, map_emissions))))

        # 7. Write the tags to file
        for i in range(len(tokens)):
            if val_tags[i] == ans_tags[i]:
                val_score += 1

print('Score: {0} out of {1}'.format(val_score, val_count))