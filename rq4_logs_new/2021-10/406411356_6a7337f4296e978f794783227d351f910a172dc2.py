user = {
    "name": "Jhon Doe",
    "info": {
        "basic": {
            "age": 25,
            "salary": 5000
        },
        "additional": {
            "study": "mathematics",
            "family": "married"
        },
        "special": {
            "projects": [
                {"name": "quantum_computer", "stage": "in_progress"},
                {"name": "laser_gun", "stage": "in_production"},
                {"name": "mathematics_library", "stage": "in_testing"}
            ]
        }
    }
}


def get_data(data_dict, keys):
    data = data_dict
    for key in keys:
        data = data[key]
    return data

def get_data_rec(data_dict, keys, index=0):
    if index < len(keys):
        return get_data_rec(data_dict[keys[index]], keys, index + 1)
    return data_dict


print(get_data(user, ['info', 'special', 'projects', 0, 'name']))
print('____________________________')
print(get_data_rec(user, ['info', 'special', 'projects', 0, 'name']))