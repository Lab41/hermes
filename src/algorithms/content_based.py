import numpy as np
import recommender_helpers as rechelp


def predict(user_info, content_array, max_prediction=1):
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

    predictions = user_prefs.cartesian(content_array).map(lambda ((user_id, user_vect), (page_id, page_vect)):\
                                    (user_id, page_id, np.dot(user_vect, page_vect)))

    #renormalize the predictions
    #this assumes that the minimum is zero which is a fair assumption
    max_val = predictions.map(lambda (user_id, page_id, pred_val): pred_val).max()
    min_val = 0
    diff = max_val-min_val

    predictions_norm = predictions.map(lambda \
                (user_id, page_id, pred_val):(user_id, page_id, (((pred_val-min_val)**0*(max_val-pred_val))/diff)*max_prediction))

    return predictions_norm
