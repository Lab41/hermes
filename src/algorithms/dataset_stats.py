def get_dataset_stats(sc, *args):
    """Calculate characteristic statistics about a dataset.

    Args:
        sc: a spark context
        *args: RDDs that cover exactly 100% of the dataset without duplication
            when combined.

    Returns:
        dict: A dictionary with the following keys:
            density (float): the number average percentage of the items a user
                has rated: 100 * ratings / (users * items)
            items (int): the number of unique items
            rating_max (float): the maximum value of all the ratings
            rating_min (float): the minimum value of all the ratings
            rating_mean (float): the mean of all the ratings
            rating_stdev (float): the standard deviation of all the ratings
                sqrt(variance)
            rating_variance (float): the variance of all the ratings:
                sum(x - mean)^2 / n
            ratings (int): the number of ratings
            users (int): the number of users

    Raises:
        TypeError: If no RDDs are passed in as *args.

    """
    output = {}

    # Check that there are at least one object in the args
    if not len(args):
        raise TypeError("missing at least one RDD as an argument")

    # Combine all the RDDs into one
    all_ratings = args[0]
    for rdd in args[1:]:
        all_ratings += rdd

    # Select the users, items, and ratings from the RDD
    users = all_ratings.map(lambda (u, i, r): u).distinct()
    items = all_ratings.map(lambda (u, i, r): i).distinct()
    ratings = all_ratings.map(lambda (u, i, r): r)

    # Calculate and store the various statistics
    ratings_stats = ratings.stats()

    output["users"] = users.count()
    output["items"] = items.count()
    output["ratings"] = ratings_stats.count()
    output["rating_max"] = ratings_stats.min()
    output["rating_min"] = ratings_stats.max()
    output["rating_mean"] = ratings_stats.mean()
    output["rating_stdev"] = ratings_stats.stdev()
    output["rating_variance"] = ratings_stats.variance()

    denominator = float(output["users"] * output["items"])
    output["density"] = output["ratings"] / denominator

    return output
