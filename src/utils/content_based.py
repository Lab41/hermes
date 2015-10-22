import numpy as np


def predict(user_info, content_array):
    """
    Creates a user preference profile by determining the rating of a particular vector item
    For example if we are looking at movies and a user highly rates sci-fi movies over drama, then the sci-fi row will be a higher number than the drama row
    Then this user preference vector is used to determine a prediction rating for each product

    There needs to be some cleaning still to renormalize the final answer so that predictions go from 0-1
    Something similar to this is shown in sum_components

    Args:
        user_info: user rdd of ratings (or interactions) which should be in the format of (user, item, rating)
        content_array: content feature array of the items which should be in the format of (item [content_feature vector])

    Returns:
        predictions: an rdd which is in the format of (user, item, predicted_rating)
    """

    user_keys = user_info.map(lambda (user, page, value): (page, (user, value)))
    user_prefs = content_array.join(user_keys).groupBy(lambda (page, ((array), (user, rating))): user)\
        .map(lambda(user, array): (user,sum_components(array)))

    predictions = user_prefs.cartesian(content_array).map(lambda ((user_id, user_vect), (page_id, page_vect)):\
                                    (user_id, page_id, vect_mult(user_vect, page_vect)))

    return predictions



def sum_components(array):
    info = []
    ratings = []
    for a in array:
        ratings.append(a[1][1][1])
        info.append(a[1][0])

    rated_info = []
    info_arr = np.array(info)
    r_arr = np.array(ratings)
    for i in range(len(r_arr)):
        r_i = info_arr[i]*r_arr[i]
        rated_info.append(r_i)

    array_out = map(sum,zip(*np.array(rated_info)))
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


def vect_mult(user_vect, page_vect):
    prod = np.multiply(page_vect, user_vect)
    #dividing by the length of the vector and then multiplying by two renormalizes
    #different normalization method could be used here
    return sum(prod)/len(prod)*2