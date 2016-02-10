import random


def predict(user_info, num_partitions=30):
    """ Randomly assign predicted ratings.

    For every possible user and item pair we randomly assign a rating drawn
    from a flat distribution with minimum equal to the minimum of the input
    ratings, and a maximum equal to the maximum of the input ratings.

    If all the ratings are a single number (like 1 for interactions), then the
    predictions will be generated from 0 to that number. If all the numbers are
    0, a ValueError is raised.

    Args:
        user_info (rdd): in the format of (user, item, rating)
        num_partitions (int): Number of partitions to coalesce the output into.

    Returns:
        rdd: an rdd containing randomized predictions in the form (user, item, random_rating)

    Raises:
        ValueError: if all the ratings are 0.
    """
    # Get the possible ratings
    ratings = user_info.map(lambda (user, item, rating): rating)
    ratings.cache()
    ratings_max = ratings.max()
    ratings_min = ratings.min()
    ratings.unpersist()

    # For interactions, all values will be 1, so we need to generate something
    # from 0 to 1 instead
    if ratings_min == ratings_max:
        if ratings_max == 0:
            raise ValueError("all input ratings are 0")
        else:
            ratings_min = 0

    # Get all the users and items
    distinct_users = user_info.map(lambda (user, item, rating): user).distinct()
    distinct_items = user_info.map(lambda (user, item, rating): item).distinct()

    # For every possible user and item pair, pick a random rating
    predictions = distinct_users\
        .cartesian(distinct_items).\
            map(
                lambda (user, item):
                (user, item, random.uniform(ratings_min, ratings_max))
            )\
        .filter(lambda (user, item, rating): rating)\
        .coalesce(num_partitions)

    return predictions
