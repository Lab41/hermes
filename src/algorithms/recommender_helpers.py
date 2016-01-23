import numpy as np


def sum_components(array):
    info = []
    ratings = []
    for (item, ((vector), (user, rating))) in array:
        ratings.append(rating)
        info.append(vector)

    rated_info = []
    info_arr = np.array(info)
    r_arr = np.array(ratings)
    for vector, rating in zip(info_arr, r_arr):
        r_i = rating * vector
        rated_info.append(r_i)

    array_out = map(sum, zip(*np.array(rated_info)))
    #if necessary renormalize
    min_val = min(array_out)
    max_val = max(array_out)
    diff = max_val-min_val
    if diff==0: diff=1
    array_out2 = []
    for t in array_out:
        new_val = ((t-min_val)**0*(max_val-t))/diff
        array_out2.append(new_val)

    return array_out2
