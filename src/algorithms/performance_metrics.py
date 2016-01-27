#!/usr/bin/env python

from math import sqrt, log
from operator import add
import numpy as np

# for arrays
from sklearn.metrics import classification_report
from sklearn.metrics import mean_squared_error
from sklearn.metrics import precision_recall_fscore_support
from sklearn.metrics import confusion_matrix
from pyspark.sql.types import *
from sklearn.metrics import jaccard_similarity_score
import itertools

def get_perform_metrics(y_test, y_train, y_predicted, content_array, sqlCtx, num_predictions=100, num_partitions=30):
    results = {}

    #because some of the algorithms we will use will only return n predictions per user all results should be analyazed for n recommendations
    n_predictions = predictions_to_n(y_predicted, number_recommended=num_predictions)

    results['rmse'] = calculate_rmse_using_rdd(y_test, n_predictions)
    results['mae'] = calculate_mae_using_rdd(y_test,n_predictions)
    results['pred_n'] = calculate_precision_at_n(y_test, n_predictions, number_recommended=num_predictions)

    #measures of diversity
    results['cat_diversity'] = calculate_population_category_diversity(n_predictions, content_array)
    results['ils'] = calc_ils(n_predictions, content_array, num_partitions=num_partitions)

    #measures of coverage
    results['cat_coverage'] = calculate_catalog_coverage(y_test, y_train, n_predictions)
    results['item_coverage'] = calculate_item_coverage(y_test, y_train, n_predictions)
    results['user_coverage'] = calculate_user_coverage(y_test, y_train, n_predictions)
    results['pred_coverage'] = calculate_prediction_coverage(y_test, n_predictions)

    #measures of serendipity
    results['serendipity'] = calculate_serendipity(y_train, y_test, n_predictions, sqlCtx, rel_filter=1)
    results['content_serendipity'] = calc_content_serendipity(y_test, n_predictions, content_array, sqlCtx)

    #measures of novelty
    results['novelty'] = calculate_novelty(y_train, y_test, n_predictions, sqlCtx)

    #relevancy statistics
    rel_stats = calc_relevant_rank_stats(y_test, n_predictions, sqlCtx)
    results['avg_highest_rank'] = rel_stats[0]
    results['avg_mean_rank'] = rel_stats[1]
    results['avg_lowest_rank'] = rel_stats[2]

    return results



# Accuracy of ratings predictions (aka regression metrics) =====================

# RMSE -----------------------------------------------------------------    

def calculate_rmse_using_rdd(y_actual, y_predicted):
    """
    Determines the Root Mean Square Error of the predictions. 

    Args:
        y_actual: actual ratings in the format of a RDD of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ]

    Assumptions: 
        y_actual and y_predicted are not in order.

    """

    ratings_diff_sq = ( y_predicted.map(lambda x: ((x[0], x[1]), x[2])) ).join( y_actual.map(lambda x: ((x[0], x[1]), x[2])) ) \
        .map( lambda (_, (predictedRating, actualRating)): (predictedRating - actualRating) ** 2 ) \

    sum_ratings_diff_sq = ratings_diff_sq.reduce(add)
    num = ratings_diff_sq.count()

    return sqrt(sum_ratings_diff_sq / float(num) )
        
def calculate_rmse_using_array(y_actual, y_predicted):
    """
    Determines the Root Mean Square Error of the predictions.

    Args: 
        y_actual: actual ratings in the format of an array of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of an array of [ (userId, itemId, predictedRating) ]

    Assumptions:
        y_actual and y_predicted are in the same order.

    """
    return sqrt(mean_squared_error(y_actual, y_predicted))
    #return mean_squared_error(y_actual, y_predicted) ** 0.5

# MAE ------------------------------------------------------------------

def calculate_mae_using_rdd(y_actual, y_predicted):
    """
    Determines the Mean Absolute Error of the predictions.

    Args:
        y_actual: actual ratings in the format of a RDD of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ]

    Assumptions:
        y_actual and y_predicted are not in order.

    """

    ratings_diff = ( y_predicted.map(lambda x: ((x[0], x[1]), x[2])) ).join( y_actual.map(lambda x: ((x[0], x[1]), x[2])) ) \
        .map( lambda (_, (predictedRating, actualRating)): abs(predictedRating - actualRating) ) \
    
    sum_ratings_diff = ratings_diff.reduce(add)
    num = ratings_diff.count()

    return sum_ratings_diff / float(num)

# Accuracy of usage predictions (aka classification metrics) ===================

# Performance, Recall, Fbeta Score, Support

def calculate_prfs_using_rdd(y_actual, y_predicted, average='macro'):
    """
    Determines the precision, recall, fscore, and support of the predictions.
    With average of macro, the algorithm Calculate metrics for each label, and find their unweighted mean.
    See http://scikit-learn.org/stable/modules/generated/sklearn.metrics.precision_recall_fscore_support.html for details

    A better metric for recommender systems is precision at N (also in this package)

    Args:
        y_actual: actual ratings in the format of an RDD of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of an RDD of [ (userId, itemId, predictedRating) ]

    Returns:
        precision, recall, fbeta_score, and support values

    """

    prediction_rating_pairs = y_predicted.map(lambda x: ((x[0], x[1]), x[2]))\
        .join(y_actual.map(lambda x: ((x[0], x[1]), x[2])))\
        .map(lambda ((user, item), (prediction, rating)): (user, item, prediction, rating))

    true_vals = np.array(prediction_rating_pairs.map(lambda (user, item, prediction, rating): rating).collect())
    pred_vals = np.array(prediction_rating_pairs.map(lambda (user, item, prediction, rating): prediction).collect())

    return precision_recall_fscore_support(map(lambda x: int(np.round(x)), true_vals),\
                                        map(lambda x: int(np.round(x)), pred_vals), average = average)

def calculate_precision_at_n(y_actual, y_predicted, number_recommended = 100):
    """
    Calculates the precision at N which is the number of 'relevant' items in the top-n items.
    'Relevant' here refers to items which are included in the user's ratings

    Args:
        y_actual: actual ratings in the format of an array of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].
            It is important that this is not the sorted and cut prediction RDD
        number_recommended: the number of recommended items to take into consideration

    Returns:
        item_coverage: value representing the percentage of user-item pairs that were able to be predicted

    """
    n_predictions = predictions_to_n(y_predicted, number_recommended)

    prediction_rating_pairs = n_predictions.map(lambda x: ((x[0], x[1]), x[2]))\
        .join(y_actual.map(lambda x: ((x[0], x[1]), x[2])))\
        .map(lambda ((user, item), (prediction, rating)): (user, item, prediction, rating))

    num_ratings = prediction_rating_pairs.groupBy(lambda (u,i,p,r): u).map(lambda (u,items): (u, len(items)))\
    .map(lambda (u, num_ratings): num_ratings)
    #the number of total users
    n = y_actual.groupBy(lambda (u,i,r): u).count()
    tot_num_ratings = num_ratings.reduce(add)

    precision_at_n = tot_num_ratings/float(n*number_recommended)

    return precision_at_n

def calculate_prfs_using_array(y_actual, y_predicted):
    """
    Determines the precision, recall, fscore, and support of the predictions.

    Args:
        y_actual: actual ratings in the format of an array of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of an array of [ (userId, itemId, predictedRating) ]

    Assumptions:
        y_actual and y_predicted are in the same order. 

    """

    # precision_recall_fscore_support's extra params:
    # 3rd param: labels = [-1, 0, +1]
    # 4th param: average = 'macro' / 'micro' / 'weighted'
    return precision_recall_fscore_support(y_actual, y_predicted)

# Accuracy of rankings of items ================================================

# TODO

# ============================================================================

def predictions_to_n(y_predicted, number_recommended=10):
    """
    Sorts the predicted ratings for a user then cuts at the specified N.  Useful when calculating metrics @N

    Args:
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ]
        number_recommended: the number of ratings desired for each user. default is set to 10 items

    Returns:
        sorted_predictions: RDD of the sorted and cut predictions in the form of of [ (userId, itemId, predictedRating) ]

    """

    sorted_predictions = y_predicted.groupBy(lambda row: row[0])\
        .map(lambda (user_id, ratings):(user_id,sort_and_cut(list(ratings),number_recommended)))\
        .map(lambda (user, ratings): ratings).flatMap(lambda x: x)

    def sort_and_cut(ratings_list, numberOfItems):
        sorted_vals = sorted(ratings_list, key=lambda ratings: ratings[2], reverse=True)
        sorted_vals = sorted_vals[:numberOfItems]
        return sorted_vals

    return sorted_predictions

def calculate_population_category_diversity(y_predicted, content_array):
    """
    The higher the category diversity the better.

    Function determines the total sum of the categories for all people (rating_array).
    So for a random group of users resulting in 330 predictions in MovieLens this could look like:
        [71, 34, 11, 22, 126, 128, 0, 165, 21, 0, 35, 0, 62, 100, 5, 131, 3, 0]
    The average of each component (by total number of predictions) is then taken
        [0.21, 0.1, 0.03....0]
    The component averages are summed
        2.79
    Finally a scaling factor is utilized to take into consideration the number of categories and the average categories for an item
        0.31
    This final step is to help normalize across datasets where some may have many more/less categories and/or more/less dense item categorization

    Args:
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ]. Should be the n predicted ratings
        content_array: content feature array of the items which should be in the format of (item [content_feature vector])

    Returns:
        cat_diversity:

    """
    ave_coverage = content_array.map(lambda (id, array): sum(array)).mean()
    rating_array_raw = y_predicted.keyBy(lambda row: row[1]).join(content_array)\
        .map(lambda (id, (rating, array)): array).collect()
    rating_array = map(sum,zip(*np.array(rating_array_raw)))
    cat_diversity = sum([r/float(len(rating_array_raw)) for r in rating_array])*ave_coverage/float(len(rating_array))

    return cat_diversity

def calc_ils(y_predicted, content_array, y_train=None, y_test=None,  num_partitions=50):
    """
    Intra-List Similarity is a measure of diversity by determining how similar items are in a user's recommended items.
    The similarity is based on these contect features
    In the future it could also be is based on how simiarlary items were rated by other users

    Method derived from 'Improving Recommendation Lists Through Topic Diversification'
    by C Ziegler, S McNee, J Knstan and G Lausen

    Args:

        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].
            It is important that this IS the sorted and cut prediction RDD
        content_array: content feature array of the items which should be in the format of (item [content_feature vector])
        num_partitions: Optimizer for specifying the number of partitions for the RDD to use.

    Returns:
        avg_ils: the average user's Intra-List Similarity
    """

    temp = y_predicted.map(lambda (u,i,p): (i, (u,p))).join(content_array)

    user_ils = temp.map(lambda (i,((u,p),c_a)): (u, (i, c_a))).groupByKey()\
        .map(lambda (user, item_list):(calc_user_ILS(list(item_list)))).collect()

    total_ils = sum(user_ils)
    avg_ils = total_ils/float(len(user_ils))

    return avg_ils

def calc_user_ILS(item_list):

    item_list = list(item_list)
    total_ils = 0
    total_count = 0
    for (i1, i2) in itertools.combinations(item_list, 2):
        # get similarity using the attached content (or rating) array
        pair_similarity = calc_jaccard_diff(i1[1], i2[1])
        total_ils += pair_similarity
        total_count += 1
    return float(total_ils)/total_count



def calculate_catalog_coverage(y_test, y_train, y_predicted):
    """
    Calculates the percentage of user-item pairs that were predicted by the algorithm.
    The full data is passed in as y_test and y_train to determine the total number of potential user-item pairs
    Then the predicted data is passed in to determine how many user-item pairs were predicted.
    It is very important to NOT pass in the sorted and cut prediction RDD and that the algorithm trys to predict all pairs
    The use the function 'cartesian' as shown in line 25 of content_based.py is helpful in that regard

    Args:
        y_test: the data used to test the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]
        y_train: the data used to train the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].  It is important that this is not the sorted and cut prediction RDD


    Returns:
        catalog_coverage: value representing the percentage of user-item pairs that were able to be predicted

    """

    y_full_data = y_test.union(y_train)

    prediction_count = y_predicted.count()
    #obtain the number of potential users and items from the actual array as the algorithms cannot predict something that was not trained
    num_users = y_full_data.map(lambda row: row[0]).distinct().count()
    num_items = y_full_data.map(lambda row: row[1]).distinct().count()
    potential_predict = num_users*num_items
    catalog_coverage = prediction_count/float(potential_predict)*100

    return catalog_coverage

def calculate_item_coverage(y_test, y_train, y_predicted):
    """
    Calculates the percentage of users pairs that were predicted by the algorithm.
    The full dataset is passed in as y_test and y_train to determine the total number of potential items
    Then the predicted data is passed in to determine how many users pairs were predicted.
    It is very important to NOT pass in the sorted and cut prediction RDD

    Args:
        y_test: the data used to test the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]
        y_train: the data used to train the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].  It is important that this is not the sorted and cut prediction RDD


    Returns:
        item_coverage: value representing the percentage of user ratings that were able to be predicted

    """

    y_full_data = y_test.union(y_train)

    predicted_items = y_predicted.map(lambda row: row[1]).distinct().count()
    #obtain the number of potential users and items from the actual array as the algorithms cannot predict something that was not trained
    num_items = y_full_data.map(lambda row: row[1]).distinct().count()

    item_coverage = predicted_items/float(num_items)*100

    return item_coverage

def calculate_user_coverage(y_test, y_train, y_predicted):
    """
    Calculates the percentage of users that were predicted by the algorithm.
    The full dataset is passed in as y_test and y_train to determine the total number of potential users
    Then the predicted data is passed in to determine how many users pairs were predicted.
    It is very important to NOT pass in the sorted and cut prediction RDD

    Args:
        y_test: the data used to test the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]
        y_train: the data used to train the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].  It is important that this is not the sorted and cut prediction RDD


    Returns:
        user_coverage: value representing the percentage of user ratings that were able to be predicted

    """
    y_full_data = y_test.union(y_train)

    predicted_users = y_predicted.map(lambda row: row[0]).distinct().count()
    #obtain the number of potential users and items from the actual array as the algorithms cannot predict something that was not trained
    num_users = y_full_data.map(lambda row: row[0]).distinct().count()

    user_coverage = predicted_users/float(num_users)*100

    return user_coverage

def calculate_prediction_coverage(y_actual, y_predicted):
    """
    Calculates the percentage of known user-item pairs which were predicted by the algorithm.
    It is different from the item_coverage in that only the user's actual ratings are analyzed vs all potential ratings
    In this manner it is likely that very low occuring items or users wouldn't hurt the final metric as much calculate_item_coverage will
    It is very important to NOT pass in the sorted and cut prediction RDD

    Args:
        y_actual: actual ratings in the format of an array of [ (userId, itemId, actualRating) ]
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].  It is important that this is not the sorted and cut prediction RDD


    Returns:
        item_coverage: value representing the percentage of user-item pairs that were able to be predicted
    """

    predictionsAndRatings = y_predicted.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(y_actual.map(lambda x: ((x[0], x[1]), x[2])))

    num_found_predictions = predictionsAndRatings.count()
    num_test_set = y_actual.count()

    prediction_coverage = num_found_predictions/float(num_test_set)*100

    return prediction_coverage

def calculate_serendipity(y_train, y_test, y_predicted, sqlCtx, rel_filter=1):
    """
    Calculates the serendipity of the recommendations.
    This measure of serendipity in particular is how surprising relevant recommendations are to a user

    serendipity = 1/N sum( max(Pr(s)- Pr(S), 0) * isrel(s)) over all items

    The central portion of this equation is the difference of probability that an item is rated for a user
    and the probability that item would be recommended for any user.
    The first ranked item has a probability 1, and last ranked item is zero.  prob_by_rank(rank, n) calculates this
    Relevance is defined by the items in the hold out set (y_test).
    If an item was rated it is relevant, which WILL miss relevant non-rated items.

    Higher values are better

    Method derived from the Coursera course: Recommender Systems taught by Prof Joseph Konstan (Universitu of Minesota)
    and Prof Michael Ekstrand (Texas State University)

    Args:
        y_train: actual training ratings in the format of an array of [ (userId, itemId, actualRating) ].
        y_test: actual testing ratings to test in the format of an array of [ (userId, itemId, actualRating) ].
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].
            It is important that this is not the sorted and cut prediction RDD
        rel_filter: the threshold of item relevance. So for MovieLens this may be 3.5, LastFM 0.
            Ratings/interactions have to be at or above this mark to be considered relevant

    Returns:
        average_overall_serendipity: the average amount of surprise over all users
        average_serendipity: the average user's amount of surprise over their recommended items
    """

    full_corpus = y_train.union(y_test)

    fields = [StructField("user", LongType(),True),StructField("item", LongType(), True),\
          StructField("rating", FloatType(), True) ]
    schema = StructType(fields)
    schema_rate = sqlCtx.createDataFrame(full_corpus, schema)
    schema_rate.registerTempTable("ratings")

    item_ranking = sqlCtx.sql("select item, avg(rating) as avg_rate, row_number() over(ORDER BY avg(rating) desc) as rank \
        from ratings group by item order by avg_rate desc")

    n = item_ranking.count()
    #determine the probability for each item in the corpus
    item_ranking_with_prob = item_ranking.map(lambda (item_id, avg_rate, rank): (item_id, avg_rate, rank, prob_by_rank(rank, n)))

    #format the 'relevant' predictions as a queriable table
    #these are those predictions for which we have ratings above the threshold
    y_test = y_test.filter(lambda (u,i,r): r>=rel_filter)

    predictionsAndRatings = y_predicted.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(y_test.map(lambda x: ((x[0], x[1]), x[2])))
    temp = predictionsAndRatings.map(lambda (a,b): (a[0], a[1], b[1], b[1]))
    fields = [StructField("user", LongType(),True),StructField("item", LongType(), True),\
          StructField("prediction", FloatType(), True), StructField("actual", FloatType(), True) ]
    schema = StructType(fields)
    schema_preds = sqlCtx.createDataFrame(temp, schema)
    schema_preds.registerTempTable("preds")

    #determine the ranking of predictions by each user
    user_ranking = sqlCtx.sql("select user, item, prediction, row_number() \
        over(Partition by user ORDER BY prediction desc) as rank \
        from preds order by user, prediction desc")
    user_ranking.registerTempTable("user_rankings")

    #find the number of predicted items by user
    user_counts = sqlCtx.sql("select user, count(item) as num_found from preds group by user")
    user_counts.registerTempTable("user_counts")

    #use the number of predicted items and item rank to determine the probability an item is predicted
    user_info = sqlCtx.sql("select r.user, item, prediction, rank, num_found from user_rankings as r, user_counts as c\
        where r.user=c.user")
    user_ranking_with_prob = user_info.map(lambda (user, item, pred, rank, num): \
                                     (user, item, rank, num, prob_by_rank(rank, num)))

    #now combine the two to determine (user, item_prob_diff) by item
    data = user_ranking_with_prob.keyBy(lambda p: p[1])\
        .join(item_ranking_with_prob.keyBy(lambda p:p[0]))\
        .map(lambda (item, (a,b)): (a[0], max(a[4]-b[3],0)))\

    #combine the item_prob_diff by user and average to get the average serendiptiy by user
    sumCount = data.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1]))
    serendipityByUser = sumCount.map(lambda (label, (value_sum, count)): (label, value_sum / count))

    num = float(serendipityByUser.count())
    average_serendipity = serendipityByUser.map(lambda (user, serendipity):serendipity).reduce(add)/num

    #alternatively we could average not by user first, so heavier users will be more influential
    #for now we shall return both
    average_overall_serendipity = data.map (lambda (user, serendipity): serendipity).reduce(add)/float(data.count())

    return (average_overall_serendipity, average_serendipity)

def calculate_novelty(y_train, y_test, y_predicted, sqlCtx):
    """
    Novelty measures how new or unknown recommendations are to a user
    An individual item's novelty can be calculated as the log of the popularity of the item
    A user's overal novelty is then the sum of the novelty of all items

    Method derived from 'Auraslist: Introducing Serendipity into Music Recommendation' by Y Zhang, D Seaghdha, D Quercia, and T Jambor

    Args:
        y_train: actual training ratings in the format of an array of [ (userId, itemId, actualRating) ].
        y_test: actual testing ratings to test in the format of an array of [ (userId, itemId, actualRating) ].
            y_train and y_test are necessary to determine the overall item ranking
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].
            It is important that this IS the sorted and cut prediction RDD

    Returns:

        avg_overall_novelty: the average amount of novelty over all users
        avg_novelty: the average user's amount of novelty over their recommended items
    """

    full_corpus = y_train.union(y_test)

    fields = [StructField("user", LongType(),True),StructField("item", LongType(), True),\
          StructField("rating", FloatType(), True) ]
    schema = StructType(fields)
    schema_rate = sqlCtx.createDataFrame(full_corpus, schema)
    schema_rate.registerTempTable("ratings")

    item_ranking = sqlCtx.sql("select item, avg(rating) as avg_rate, row_number() over(ORDER BY avg(rating) desc) as rank \
        from ratings group by item order by avg_rate desc")

    n = item_ranking.count()
    item_ranking_with_nov = item_ranking.map(lambda (item_id, avg_rate, rank): (item_id, (avg_rate, rank, log(max(prob_by_rank(rank, n), 1e-100), 2))))

    user_novelty = y_predicted.keyBy(lambda (u, i, p): i).join(item_ranking_with_nov).map(lambda (i,((u_p),(pop))): (u_p[0], pop[2]))\
        .groupBy(lambda (user, pop): user).map(lambda (user, user_item_probs):(np.mean(list(user_item_probs), axis=0)[1])).collect()

    all_novelty = y_predicted.keyBy(lambda (u, i, p): i).join(item_ranking_with_nov).map(lambda (i,((u_p),(pop))): (pop[2])).collect()
    avg_overall_novelty = float(np.mean(all_novelty))

    avg_novelty = float(np.mean(user_novelty))

    return (avg_overall_novelty, avg_novelty)

def prob_by_rank(rank, n):
    """
    Transforms the rank of item into the probability that an item is recommended an observed by the user.
    The first ranked item has a probability 1, and last ranked item is zero.
    Simplified version of 1- (rank-1)/(n-1)

    Args:
        rank: rank of an item
        n: number of items to be recommended

    Returns:
        prob: the probability an item will be recommended
    """

    #if there is only one item, probability should be one, but method below will not work...
    if n == 1:
        prob = 1.0
    else:
        prob = (n-rank)/float(n-1)
    return prob

def calc_content_serendipity(y_actual, y_predicted, content_array, sqlCtx):
    """
    Calculates the serendipity of the recommendations based on their content.
    This measure of serendipity in particular is how surprising relevant recommendations are to a user

    This method measures the minimum content distance between recommended items and those in the user's profile.
    Serendipity(i) = min dist(i,j) where j is an item in the user's profile and i is the recommended item
    Distance is the inverse of the Jaccard Similarity Score (1-Jaccard)
    A user's overall surprise is the average of each item's surprise.
    We could weight by p(recommend) as we did in calculate_serendipity().
    For now the sorted and cut predictions should be passed in versus the full prediction list

    This method is outlined in 'Measuring Surprise in Recommender Systems' by Marius Kaminskas and Derek Bridge

    Args:
        y_actual: actual ratings in the format of an array of [ (userId, itemId, actualRating) ].
            Only favorably rated items should be passed in (so pre-filtered)
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].
            It is important that this IS the sorted and cut prediction RDD
        content_array: content feature array of the items which should be in the format of (item [content_feature vector])

    Returns:
        average_overall_content_serendipity: the average amount of surprise over all users based on content
        avg_content_serendipity: the average user's amount of surprise over their recommended items based on content
    """

    #instead of calculating the distance between the user's items and predicted items we will do a lookup to a table with this information
    #this minimizes the amount of repeated procedures
    ##TODO only look at one half of the matrix as we don't need (a,b, dist) if we have (b,a, dist). Need to modify lower section of code to do this
    content_array_matrix = content_array.cartesian(content_array).map(lambda (a, b): (a[0], b[0], calc_jaccard_diff(a[1], b[1])))


    #create a matrix of all predictions for each item a user has rated
    user_prod_matrix = y_actual.keyBy(lambda (u,i,r): u).join(y_predicted.keyBy(lambda (u,i,p):u))

    #determine all distances for the predicted items for a user in the format of [user, rec_item, dist]
    user_sim = user_prod_matrix.map(lambda (u, (t, p)): ((t[1],p[1]), u))\
            .join(content_array_matrix.map(lambda (i1, i2, dist): ((i1,i2),dist)))\
            .map(lambda (items, user_dist): (user_dist[0], items[1], user_dist[1]))

    user_sim.cache()

    #while we can certainly do the rest in RDD land, it will be easier if the table were queriable
    fields = [StructField("user", LongType(),True),StructField("item", LongType(), True),\
              StructField("dist", FloatType(), True) ]
    schema = StructType(fields)
    user_sim_sql = sqlCtx.createDataFrame(user_sim, schema)
    user_sim_sql.registerTempTable("user_sim")

    #determine the minimum distance for each recommended item
    user_item_serendip = sqlCtx.sql("select user, item, min(dist) as min_dist from user_sim group by user, item")
    user_item_serendip.registerTempTable("user_item_sim")

    #now determine the average minimum distance over all recommended items for a user
    user_serendip = sqlCtx.sql("select user, avg(min_dist) from user_item_sim group by user")

    num_users = sqlCtx.sql("select distinct(user) from user_item_sim").count()
    avg_content_serendipity = user_serendip.map(lambda (user, sim): sim).reduce(add)/float(num_users)

    #alternatively we could average not by user first, so heavier users will be more influential
    #for now we shall return both
    average_overall_content_serendipity = sqlCtx.sql("select avg(min_dist) from user_item_sim").collect()[0][0]

    return (average_overall_content_serendipity, avg_content_serendipity)

def calc_jaccard_diff(array_1, array_2):
    """
    Utilizes the Jaccard Similarity Score from scikitlearn to determine distance between two arrays
    http://scikit-learn.org/stable/modules/generated/sklearn.metrics.jaccard_similarity_score.html

    These arrays for example could be two content vectors.
    This function would then determine how dis-similar they are to each other

    Args:
        array_1: array number one.  For example: [0, 1, 1, 1]
        array_2: array number two.  For example: [0, 1, 0, 1]

    Returns:
        dist: the inverse of the jaccard similarity.  For the above this equals 0.25
    """
    j=jaccard_similarity_score(array_1, array_2)
    dist = 1-j
    #it is very important that we return the distance as a python float
    #otherwise a numpy float is returned which causes chaos and havoc to ensue
    return float(dist)

def calc_relevant_rank_stats(y_actual, y_predicted, sqlCtx):
    """
    Determines the average minimum, average and maximum ranking of 'relevant' items
    'Relevant' here means that the item was rated, i.e., it exists in the y_actual RDD

    Args:
        y_actual: actual ratings in the format of a RDD  of [ (userId, itemId, actualRating) ].
        y_predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].
            It is important that this IS NOT the sorted and cut prediction RDD

    Returns:
        average_overall_content_serendipity:
    """

    predictions2 = y_predicted.map(lambda (u,i,p): (u,i,p))

    fields = [StructField("user", LongType(),True),StructField("item", LongType(), True),\
      StructField("prediction", FloatType(), True) ]
    schema = StructType(fields)
    schema_preds = sqlCtx.createDataFrame(predictions2, schema)
    schema_preds.registerTempTable("predictions")

    #determine each user's prediction ratings
    prediction_ranking = sqlCtx.sql("select p.user, p.item, p.prediction, row_number() \
    over(Partition by p.user ORDER BY p.prediction desc) as rank \
    from predictions p order by p.user, p.prediction desc")
    prediction_ranking.registerTempTable("prediction_rankings")

    fields = [StructField("user", LongType(),True),StructField("item", LongType(), True),\
      StructField("rating", FloatType(), True) ]
    rating_schema = StructType(fields)
    rating_schema_preds = sqlCtx.createDataFrame(y_actual, schema)
    rating_schema_preds.registerTempTable("ratings")

    relevant_ranks = sqlCtx.sql("select p.user, r.item, p.rank from prediction_rankings p, ratings r \
    where r.user=p.user and p.item=r.item")
    relevant_ranks.registerTempTable("relevant_ranks")

    max_ranks = sqlCtx.sql("select min(rank), avg(rank), max(rank) from relevant_ranks group by user")
    max_ranks_local = max_ranks.collect()

    rank_stats = np.mean(max_ranks_local, axis=0)

    return rank_stats