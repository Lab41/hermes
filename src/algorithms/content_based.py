import numpy as np
import recommender_helpers as rechelp
from numpy.linalg import norm


def predict(user_info, content_array, num_partitions=30):
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
        predictions_norm: an rdd which is in the format of (user, item, predicted_rating) normalized to be between 0 and the max prediction
    """

    user_keys = user_info.map(lambda (user, page, value): (page, (user, value)))
    user_prefs = content_array.join(user_keys).groupBy(lambda (page, ((array), (user, rating))): user)\
        .map(lambda(user, array): (user, rechelp.sum_components(array)))

    #ensure that there are no user_preference vectors or content vectors with a zero array - this causes the predictions to be nan
    user_prefs = user_prefs.filter(lambda (u_id,user_vect ): all(v == 0 for v in list(user_vect))==False)
    content_array = content_array.filter(lambda (c_id, cont_vect ): all(v == 0 for v in list(cont_vect))==False)

    max_rating = user_info.map(lambda (user, item, rating): rating).max()
    min_rating = user_info.map(lambda (user, item, rating): rating).min()

    if max_rating == min_rating:
        min_rating=0

    diff_ratings = float(max_rating - min_rating)

    predictions = user_prefs.cartesian(content_array).map(lambda ((user_id, user_vect), (page_id, item_vector)):\
            (user_id, page_id, np.dot(user_vect, item_vector)/(norm(item_vector)*norm(user_vect)))).coalesce(num_partitions)

    max_pred = predictions.map(lambda (user,item, pred):pred).max()
    min_pred = predictions.map(lambda (user,item, pred):pred).min()

    diff_pred = float(max_pred - min_pred)

    norm_predictions = predictions.map(lambda (user,item, pred):(user, item, \
                    (pred-min_pred)*float(diff_ratings/diff_pred)+min_rating))

    return norm_predictions
