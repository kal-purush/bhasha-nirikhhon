import enchant

# Initialize English dictionary
english_dict = enchant.Dict("en_US")

# Digit-to-letter mapping
digit_to_letters = {
    '2': 'ABC', '3': 'DEF', '4': 'GHI', '5': 'JKL',
    '6': 'MNO', '7': 'PQRS', '8': 'TUV', '9': 'WXYZ'
}

# Memoization dictionary for combinations
combination_cache = {}

###########################################################################################################################################
# Generate letter combinations for a given phone number section
def generate_combinations(phone_section):
    if not phone_section:
        return [""]  # Base case: single empty combination

    # Check memoization cache
    if phone_section in combination_cache:
        return combination_cache[phone_section]

    first_digit = phone_section[0]
    remaining_section = phone_section[1:]
    letters = digit_to_letters.get(first_digit, "")

    combinations = []
    remaining_combinations = generate_combinations(remaining_section)

    for letter in letters:
        combinations.extend(letter + combo for combo in remaining_combinations)

    # Store in cache
    combination_cache[phone_section] = combinations
    return combinations

############################################################################################################################################
# Generate combinations for specific sections
def generate_10_digit_combinations(phone_number):
    return generate_combinations(phone_number)

def generate_7_digit_whole_combinations(phone_number):
    return generate_combinations(phone_number[3:])

def generate_7_digit_split_combinations(phone_number):
    # Avoid redundant recomputation by precomputing parts
    part1 = generate_combinations(phone_number[3:6])
    part2 = generate_combinations(phone_number[6:])
    return [(p1, p2) for p1 in part1 for p2 in part2]

def generate_exchange_only_combinations(phone_number):
    return generate_combinations(phone_number[3:6])

def generate_number_only_combinations(phone_number):
    return generate_combinations(phone_number[6:])

############################################################################################################################################
# Check if a given combination is a valid English word
def is_word(combination):
    return english_dict.check(combination)

############################################################################################################################################
# Generate meaningful words based on condition
def generateValidWords(condition, phone_number):
    condition_functions = {
        "10-digit": generate_10_digit_combinations,
        "7-digit whole": generate_7_digit_whole_combinations,
        "7-digit split": generate_7_digit_split_combinations,
        "exchange only": generate_exchange_only_combinations,
        "number only": generate_number_only_combinations
    }

    # Get the appropriate function
    generate_combinations_func = condition_functions.get(condition)
    possible_combinations = generate_combinations_func(phone_number) if generate_combinations_func else []

    # Validate combinations as English words
    if condition == "7-digit split":
        return [(p1, p2) for p1, p2 in possible_combinations if is_word(p1) and is_word(p2)]
    else:
        return [combination for combination in possible_combinations if is_word(combination)]

############################################################################################################################################
# Main function to take input and process the phone number
def main():
    while True:
        phone_number = input("Enter a 10-digit phone number (excluding leading 1): ")

        # Validate input
        if len(phone_number) == 11 and phone_number[0] 