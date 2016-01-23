import numpy as np
from numpy.linalg import norm


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


def sort_and_cut_by_cluster(row, N, fractions):
    """Pick the top N items from a cluster.

    This function returns the top N * fractions[cluster] items

    Args:
        row (rdd object): row has the form
            ((user, cluster), iterator_over((user, cluster, item, rating)))
        N (int): number of items desired in to be recommended in total
        fractions (dict): map of cluster to fraction of the total dataset that
            is represented by that cluster.

    Returns:
        list of tuples: The tuples have the form (user, rating, item)
    """
    cluster = row[0][1]
    to_take = round(N * fractions[cluster])
    content = ((user, rating, item) for (user, _, item, rating) in row[1])
    output = []
    i = 0
    for tup in sorted(content, reverse=True):
        if i == to_take:
            return output
        output.append(tup)
        i += 1

    return output


def dot_product_predict_ratings(uv, iv, minimum=0., maximum=5.):
    """Calculates a predicted rating for an item using the dot product.

    The distance between two vectors is computed with the dot product. The
    returned rating is scaled to fall within the same range as the ratings of
    the dataset.

    Args:
        uv (numpy array): user profile vector
        iv (numpy array): item content vector
        minimum (float): the minimum value allowed for a rating
        maximum (float): the maximum value allowed for a rating

    Returns:
        float: a predicted rating

    """
    angle = np.dot(uv, iv)/(norm(iv)*norm(uv))
    shift = minimum + 1  # +1 is from - -1 is the minimum of our original distribution
    scale = (maximum - minimum) / 2.  # 2. is 1 - -1, the range of our original distribution
    return (angle + shift) * scale


def compute_user_vector_with_threshold(array, threshold=3.5):
    """Compute a user profile by summing only vectors from items with a
    positive review.

    Item vectors with a rating above a set threshold are included, other
    vectors are discarded. The user profile is not normalized.

    Args:
    array (rdd object): an iterator over objects of the form
        (item, ((vector), (user, rating)))
    threshold (float, default 3.5): Cut off for a good rating score, ratings
        below this value are discarded.

    Returns:
        numpy array: user profile
    """
    vec = None
    for (item, ((vector), (user, rating))) in array:
        # Allow us to weight vectors by threshold
        if rating >= threshold:
            sign = 1
        else:
            sign = 0
        # Add the vector to the user's profile
        if vec is not None:
            vec += sign * vector
        else:
            vec = sign * vector
    return vec
