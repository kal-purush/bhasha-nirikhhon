
# create indexed nested dict.
def nested_dict(*args) -> dict:
    """
    receives dicts & return nested dict
    :param args: dict
    :return: list of dicts.
    """

    data_set = {i: args[i] for i in range(len(args))}
    return data_set


# A.
def split_male_female(dict_people: dict):
    """ received list of dict with people objects male and  female,
    and return tow dicts one for male sec for female """

    dict_of_male = {}
    dict_of_female = {}
    key_male = 0
    key_female = 0
    for i in range(len(dict_people)):
        if dict_people[i]["gender"] == "male":
            dict_of_male[key_male] = dict_people[i]
            key_male += 1
        else:
            dict_of_female[key_female] = dict_people[i]
            key_female += 1

    return dict_of_female, dict_of_male


# B.
def find_median_average(dikt_people: dict):
    """
    received dict of people and return the average of the ages, and the median of the ages.
    :param dikt_people: dict
    :return: avg_ages: float, med_ages int
    """
    # var for devide the avg.
    avg_ages = 0
    # var for sum of all ages.
    sum_age = 0

    lst_ages = []
    # looping to count the sum of average.
    for i in range(len(dikt_people)):
        sum_age += dikt_people[i]["age"]
        # find the average.
        avg_ages = sum_age/len(dikt_people)
        lst_ages.append(dikt_people[i]["age"])
        lst_ages = sorted(lst_ages)

    # find median.
    med_ages = len(lst_ages)//2
    if med_ages % 2 == 0:
        med_ages = lst_ages[med_ages-1]
    else:
        med_ages = lst_ages[med_ages]
    return avg_ages, med_ages


# C.
def print_values_above(people: dict, num=0):


        """
        received dickt of people and int number, if number > 0 print all the people with age bigger than number,
        else print all the dict.
        :param people: dict
        :param num: int
        :return: print people from the dict.
        """
        if num > 0:
            for i in range(len(people)):
                if people[i]["age"] > num:
                    print(people[i])
        else:
            for i in range(len(people)):
                print(people[i])


def main():
    dict_1 = {"name": "John", "gender": "male", "age": 32}
    dict_2 = {"name": "Mike", "gender": "male", "age": 21}
    dict_3 = {"name": "Alex", "gender": "male", "age": 40}
    dict_4 = {"name": "Marina", "gender": "female", "age": 54}
    data_set = nested_dict(dict_1, dict_2, dict_3, dict_4)
    print_values_above(data_set, 32)
    dict_female, dict_male = split_male_female(data_set)

    avg_ages, median_ages = find_median_average(data_set)
    print("Average of ages:", avg_ages, "median of ages:", median_ages)

if __name__ == "__main__":
    main()