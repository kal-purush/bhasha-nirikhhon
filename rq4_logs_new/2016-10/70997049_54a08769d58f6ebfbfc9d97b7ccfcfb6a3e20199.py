import config
import general_functions


def percent_in_range(touches, medial_axis):
    count_touches = len(touches)
    distances = map(lambda x: general_functions.find_minimum_distance(x, medial_axis), touches)
    filtered_distances = list(filter(lambda x: x <= config.radius_threshold, distances))
    return float(len(filtered_distances) / count_touches * 100)


