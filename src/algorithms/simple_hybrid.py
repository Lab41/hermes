def predict(predictions_vector_0, predictions_vector_1, mixing_variable=0.5, num_partitions=30):
    """Apply a simple linear hybrid recommender.

    This function implements the simple linear hybrid recommender Zhou et. al:

    "Solving the apparent diversity-accuracy dilemma of recommender systems"
    http://arxiv.org/pdf/0808.2670.pdf Equation 5

    It takes the weighted linear average of two previous recommendations and
    uses the new average as its own predicted rating:

        new_rating = mixing_variable * rating_a + (1 - mixing_variable) * rating_b

    Args:
        predictions_vector_0 (rdd): Contains prediction tuples of the form
            (user_id, item_id, predicted_rating) generated from a recommender
            algorithm.
        predictions_vector_1 (rdd): Contains prediction tuples of the form
            (user_id, item_id, predicted_rating) generated from a second
            recommender algorithm.
        mixing_variable (float): A float in the range [0., 1.] which determines
            how to weight the two predictions. If `mixing_variable` is 0 then
            `predictions_vector_0` is given all the weight (and
            `predictions_vector_1` is ignored). If `mixing_variable` is 1 then
            `predictions_vector_1` is given all the weight (and
            `predictions_vector_0` is ignored). Defaults to 0.5.
        num_partitions (int): The number of partitions to use for the returned
            data. Defaults to 30.

    Returns:
        rdd: An rdd containing prediction tuples of the form
            (user_id, item_id, rating)

    Raises:
        ValueError: If `mixing_variable` is not within the range [0, 1]

    """
    # Check the mixing_variable is set to an acceptable value
    if not 0 <= mixing_variable <= 1:
        raise ValueError('mixing_variable must be within the range [0, 1]')

    # Short-circuit in the trivial cases
    if mixing_variable == 0:
        return predictions_vector_0
    elif mixing_variable == 1:
        return predictions_vector_1

    # Otherwise calculate the linear average
    keyed_vector_0 = predictions_vector_0\
        .map(lambda (u, i, r): ((u, i), r))
    keyed_vector_1 = predictions_vector_1\
        .map(lambda (u, i, r): ((u, i), r))

    predictions = keyed_vector_0.join(keyed_vector_1)\
        .map(lambda ((u, i), (r0, r1)): (u, i, (1. - mixing_variable) * r0 + mixing_variable * r1))\
        .coalesce(num_partitions)

    return predictions
