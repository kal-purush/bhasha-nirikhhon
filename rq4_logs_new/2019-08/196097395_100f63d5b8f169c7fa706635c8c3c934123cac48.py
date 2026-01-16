import numpy as np
from numpy import array
def load_constants(file_name):
    f = open(file_name, 'r')
    arr = f.readlines()
    constants = []
    for x in arr:
        s = x.replace("\n", '');
        constants.append(s)
    return constants

def one_hot_encode(user_preferences, constants_arr):
    one_hot_encode = [0] * len(constants_arr)
    for preference in user_preferences:
        if isinstance(preference, str):
            idx = constants_arr.index(preference)
            one_hot_encode[idx] = 1
        else:
            idx = constants_arr.index(preference['name'])
            one_hot_encode[idx] = float(preference['skill']) / 10.0
    return one_hot_encode

def compute_preference_score(user_preference, opponent_preference, constants_arr):
    user = array(one_hot_encode(user_preference, constants_arr))
    opponent = array(one_hot_encode(opponent_preference, constants_arr))
    result = np.multiply(user, opponent)
    return result

def score_arr_to_scalar(score_arr):
    total = 0
    for score in score_arr:
        if score > 0: total += score
    return total

def match(user, other_user, care_score):
	interests = load_constants('/Users/admin/Desktop/Projects/hm/server/tests/interests.txt')
	languages = load_constants('/Users/admin/Desktop/Projects/hm/server/tests/languages.txt')
	fields = load_constants('/Users/admin/Desktop/Projects/hm/server/tests/fields.txt')
	technologies = load_constants('/Users/admin/Desktop/Projects/hm/server/tests/technologies.txt')

	interest_score = score_arr_to_scalar(compute_preference_score(user['interests'], other_user['interests'], interests))
	language_score = score_arr_to_scalar(compute_preference_score(user['languages'], other_user['languages'], languages))
	technology_score = score_arr_to_scalar(compute_preference_score(user['technologies'], other_user['technologies'], technologies))
	field_score = score_arr_to_scalar(compute_preference_score(user['fields'], other_user['fields'], fields))

	similiarity_score = care_score['interests'] * interest_score / 10.0 + care_score['languages'] * language_score +
						care_score['technologies'] * technologies_score / 10.0 + care_score['fields'] * field_score
	return similiarity_score