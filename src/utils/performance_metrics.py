from math import sqrt


def caluculate_rmse(predictions, user_ratings):
    """
    Determines the Root Mean Square Error of the predictions

    Args:
        predictions: Predictions rdd which should be in the format of (user, item, predicted_rating)
        user_ratings: The actual ratings which should be in the format of (user, item, rating)

    Returns:
        rmse: RMSE between predicted and actual rating
    """

    predict_diffs = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(user_ratings.map(lambda x: ((x[0], x[1]), x[2])))\
    .map(lambda (a, (predict,act)): ((predict-act)**2)).collect()
    n = len(predict_diffs)
    rmse = sqrt(sum(predict_diffs))/float(n)
    return rmse