import numpy as np
from src.algorithms import recommender_helpers as rechelp
from pyspark.sql.types import *
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from operator import add
from sklearn.metrics.pairwise import cosine_similarity

def calc_cf_mllib(y_training_data, num_partitions = 20):
    """
    Utilizes the ALS collaborative filtering algorithm in MLLib to determine the predicted ratings

    Args:
        y_training_data: the data used to train the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]

    Returns:
        predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].

    """

    #Predicted values can be anywhere - because we are normalizing the content based algorithms we should likely normalize here
    max_rating = y_training_data.map(lambda user_item_rating: user_item_rating[2]).max()
    min_rating = y_training_data.map(lambda user_item_rating2: user_item_rating2[2]).min()

    if max_rating == min_rating:
        min_rating=0

    #MLLIb has two methods, train and trainImplicit().  Implicit data will go between zero and 1
    if min_rating==0 and max_rating==1:
        model = ALS.trainImplicit(y_training_data, rank = 10, iterations = 5)
    else:
        model = ALS.train(y_training_data, rank = 10, iterations = 5)

    #predict all user, item pairs
    item_ids = y_training_data.map(lambda u_i_r3: u_i_r3[1]).distinct()
    user_ids = y_training_data.map(lambda u_i_r4: u_i_r4[0]).distinct()
    user_item_combo = user_ids.cartesian(item_ids).coalesce(num_partitions)

    predicted = model.predictAll(user_item_combo.map(lambda x: (x[0], x[1])))

    norm_predictions = predicted.map(lambda user_item_pred: (user_item_pred[0],user_item_pred[1], rechelp.squish_preds(user_item_pred[2],min_rating,max_rating)))


    return norm_predictions


def calc_user_user_cf(training_data, sqlCtx, num_partitions=20):
    """
    A very simple user-user CF algorithm in PySpark. Method is less stable than calc_user_user_cf2

    Method derived from the Coursera course: Recommender Systems taught by Prof Joseph Konstan (Universitu of Minesota)
    and Prof Michael Ekstrand (Texas State University)

    Args:
        y_training_data: the data used to train the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]

    Returns:
        predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].

    """

    user_groups = training_data.groupBy(lambda user_item_rating5: user_item_rating5[0])

    user_groups_sim = user_groups.cartesian(user_groups).map(lambda user1_id_user1_rows_user2_id_user2_rows:\
        (user1_id_user1_rows_user2_id_user2_rows[0][0], user1_id_user1_rows_user2_id_user2_rows[1][0], similarity(user1_id_user1_rows_user2_id_user2_rows[0][1], user1_id_user1_rows_user2_id_user2_rows[1][1], 1))).coalesce(num_partitions)
    fields = [StructField("user1", LongType(),True),StructField("user2", LongType(), True),\
              StructField("similarity", FloatType(), True) ]
    schema_sim = StructType(fields)
    user_sim = sqlCtx.createDataFrame(user_groups_sim, schema_sim)
    user_sim.registerTempTable("user_sim")

    fields = [StructField("user", LongType(),True),StructField("item", LongType(), True),\
              StructField("rating", FloatType(), True) ]
    schema = StructType(fields)
    user_sim_sql = sqlCtx.createDataFrame(training_data, schema)
    user_sim_sql.registerTempTable("ratings")

    avg_ratings = sqlCtx.sql("select user, avg(rating) as avg_rating from ratings group by user")
    avg_ratings.registerTempTable("averages")

    residual_ratings = sqlCtx.sql("select r.user, r.item, (r.rating-a.avg_rating) as resids from ratings r, \
        averages a where a.user=r.user")
    residual_ratings.registerTempTable("residuals")

    user_sim_resids = sqlCtx.sql("select u.user2, r.user, r.item, r.resids, similarity, r.resids*similarity as r_w  from residuals r, \
        user_sim u  where r.user=u.user1")
    user_sim_resids.registerTempTable("user_sim_resids")

    item_adjusts = sqlCtx.sql("select user2, item, sum(r_w)/sum(abs(similarity)) as item_adj from user_sim_resids group by user2, item")
    item_adjusts.registerTempTable("item_adjusts")

    predictions = sqlCtx.sql("select user2 as user, item, (avg_rating +item_adj) as pred_rating \
        from item_adjusts i, averages a where a.user=i.user2")

    return predictions

def calc_user_user_cf2(training_data, num_partitions=20):
    """
    A very simple user-user CF algorithm in PySpark. Method is more stable than calc_user_user_cf

    Method derived from the Coursera course: Recommender Systems taught by Prof Joseph Konstan (Universitu of Minesota)
    and Prof Michael Ekstrand (Texas State University)

    Args:
        y_training_data: the data used to train the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]

    Returns:
        predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].

    """

    user_groups = training_data.groupBy(lambda user_item_rating6: user_item_rating6[0])

    user_groups_sim = user_groups.cartesian(user_groups).map(lambda user1_id_user1_rows_user2_id_user2_rows7:\
        (user1_id_user1_rows_user2_id_user2_rows7[0][0], user1_id_user1_rows_user2_id_user2_rows7[1][0], similarity(user1_id_user1_rows_user2_id_user2_rows7[0][1], user1_id_user1_rows_user2_id_user2_rows7[1][1], 1))).coalesce(num_partitions)

    user_averages = training_data.map(lambda user_item_rating8: (user_item_rating8[0], (user_item_rating8[2]))).groupByKey().\
        map(lambda user_ratings: (user_ratings[0], np.mean(list(user_ratings[1]))))

    user_resids = training_data.map(lambda user_item_rating9: (user_item_rating9[0], (user_item_rating9[1], user_item_rating9[2]))).join(user_averages)\
        .map(lambda user_item_rating_avg_rating: (user_item_rating_avg_rating[0], (user_item_rating_avg_rating[1][0][0], user_item_rating_avg_rating[1][0][1]-user_item_rating_avg_rating[1][1])))

    item_adjustments = user_resids.join(user_groups_sim.map(lambda u1_u2_sim: (u1_u2_sim[0], (u1_u2_sim[1], u1_u2_sim[2]))))\
        .map(lambda u1_item_resid_u2_sim: ((u1_item_resid_u2_sim[1][1][0],u1_item_resid_u2_sim[1][0][0]), (u1_item_resid_u2_sim[1][0][1]*u1_item_resid_u2_sim[1][1][1], u1_item_resid_u2_sim[1][1][1]))).\
        groupByKey().map(lambda user_item_sim_list: (user_item_sim_list[0][0], user_item_sim_list[0][1], calc_item_adjust(user_item_sim_list[1])))

    predictions = item_adjustments.map(lambda user_item_item_adj: (user_item_item_adj[0], (user_item_item_adj[1], user_item_item_adj[2]))).join(user_averages)\
        .map(lambda user_item_item_adj_avg_rate: (user_item_item_adj_avg_rate[0], user_item_item_adj_avg_rate[1][0][0], user_item_item_adj_avg_rate[1][1]+user_item_item_adj_avg_rate[1][0][1]))

    #Predicted values can be anywhere - because we are normalizing the content based algorithms we should likely normalize here
    max_rating = training_data.map(lambda user_item_rating10: user_item_rating10[2]).max()
    min_rating = training_data.map(lambda user_item_rating11: user_item_rating11[2]).min()

    if max_rating == min_rating:
        min_rating=0

    norm_predictions = predictions.map(lambda user_item_pred12: (user_item_pred12[0],user_item_pred12[1], rechelp.squish_preds(user_item_pred12[2],min_rating,max_rating)))

    return norm_predictions

def calc_item_adjust(sim_resids):
    #data coming into this function is a list of [residual*similarity, similarity] for all user, item paris
    #we want to output sum(residual*similarity)/sum(abs(similarity))
    sum_r_w = 0
    sum_sim = 0
    for s in sim_resids:
        sum_r_w += s[0]
        sum_sim += abs(s[1])

    if sum_sim ==0:
        return 0
    else:
        return sum_r_w/float(sum_sim)

def calc_item_item_cf(training_data, num_partitions):
    """
    A very simple item-item CF algorithm in PySpark.

    Method derived from the Coursera course: Recommender Systems taught by Prof Joseph Konstan (Universitu of Minesota)
    and Prof Michael Ekstrand (Texas State University)

    Args:
        y_training_data: the data used to train the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]

    Returns:
        predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].

    """

    item_groups = training_data.groupBy(lambda user_item_rating13: user_item_rating13[1])
    item_similarity = item_groups.cartesian(item_groups).map(lambda item1_id_item1_rows_item2_id_item2_rows:\
                       (item1_id_item1_rows_item2_id_item2_rows[0][0], item1_id_item1_rows_item2_id_item2_rows[1][0], similarity(item1_id_item1_rows_item2_id_item2_rows[0][1], item1_id_item1_rows_item2_id_item2_rows[1][1], 0))).coalesce(num_partitions)

    user_item_sim = training_data.keyBy(lambda user_item_rating14: user_item_rating14[1])\
        .join(item_similarity.keyBy(lambda item1_item2_sim: item1_item2_sim[0]))\
        .map(lambda a:((a[1][0][0], a[1][1][1]), (a[1][1][0],a[1][0][2],a[1][1][2])))\
        .filter(lambda user_item2_item_rating_sim: user_item2_item_rating_sim[0][1]!=user_item2_item_rating_sim[1][0])

    predictions = user_item_sim.groupByKey()\
        .map(lambda user_item_rows: (user_item_rows[0][0], user_item_rows[0][1], get_item_prob(user_item_rows[1])))

    #Predicted values can be anywhere - because we are normalizing the content based algorithms we should likely normalize here
    max_rating = training_data.map(lambda user_item_rating15: user_item_rating15[2]).max()
    min_rating = training_data.map(lambda user_item_rating16: user_item_rating16[2]).min()

    if max_rating == min_rating:
        min_rating=0

    norm_predictions = predictions.map(lambda user_item_pred17: (user_item_pred17[0],user_item_pred17[1], rechelp.squish_preds(user_item_pred17[2],min_rating,max_rating)))

    return norm_predictions

def similarity(item1_rows, item2_rows, index):
    #to determine user similarity index=0
    #to determine item similarity index=1
    rating_match = []
    for i in item1_rows:
        for j in item2_rows:
            if i[index]==j[index]:
                rating_match.append((i[2],j[2]))

    if len(rating_match)==0:
        sim = 0.0
    else:
        sim = cosine_similarity(*list(zip(*rating_match)))[0][0]

    return float(sim)

def get_item_prob(rows):
    nom = 0
    denom = 0
    for r in rows:
        nom += r[1]*r[2]
        denom += abs(r[2])

    if denom ==0:
        return 0
    else:
        item_prob = nom/float(denom)
        return float(item_prob)


def calc_naive_bayes_using_pyspark(training_data, num_partitions=20):
    """
    Determine the predicted rating of every user-item combination using MLlib's Naive Bayes algorithm.

    Args:
        training_data: the data used to train the RecSys algorithm in the format of a RDD of [ (userId, itemId, actualRating) ]

    Returns:
        predictions: predicted ratings of every user-item combination in the format of a RDD of [(userId, itemId, predictedRating)].
    """

    # to use MLlib's Naive Bayes model, it requires the input to be in a format of a LabeledPoint
    # therefore, convert dataset so that it will in the format [(rating, (user, item))]
    r_ui_train = training_data.map(lambda u_i_r18: LabeledPoint(u_i_r18[2], (u_i_r18[0], u_i_r18[1])))
    # train Naive Bayes model
    naiveBayesModel = NaiveBayes.train(r_ui_train, lambda_=1.0)
    # predict on all user-item pairs
    user_ids = training_data.map(lambda u_i_r19: u_i_r19[0]).distinct()
    item_ids = training_data.map(lambda u_i_r20: u_i_r20[1]).distinct()
    ui_combo = user_ids.cartesian(item_ids).coalesce(num_partitions)
    r_ui_combo = ui_combo.map(lambda u_i_r21: LabeledPoint(1, (u_i_r21[0], u_i_r21[1])))
    # make prediction
    predictions = r_ui_combo.map(lambda p: (p.features[0], p.features[1], naiveBayesModel.predict(p.features)))

    return predictions

def calc_naive_bayes_components(training_data, sc, num_partitions=20):
    """
    Helper function that will compute the necessary components needed by:
    calc_naive_bayes_map(), calc_naive_bayes_mse(), calc_naive_bayes_mae()

    Args:
        training_data: the data used to train the RecSys algorithm in the format of a RDD of [ (userId, itemId, actualRating) ]
        sc: spark context

    Returns:
        ui_allBayesProb: computation of all bayes probability for each user-item pairs in the format [((userId, itemId), (rating, bayesProbability)]

    """

    # create RDD for range of ratings, ie. [1, 2, 3, 4, 5] for ratings 1-5
    min_rating = training_data.map(lambda u_i_r22: u_i_r22[2]).min()
    max_rating = training_data.map(lambda u_i_r23: u_i_r23[2]).max()
    range_of_ratings = sc.broadcast(list(range(int(min_rating), int(max_rating + 1))))

    #predict on all user-item pairs
    user_ids = training_data.map(lambda u_i_r24: u_i_r24[0]).distinct()
    item_ids = training_data.map(lambda u_i_r25: u_i_r25[1]).distinct()
    ui_combo = user_ids.cartesian(item_ids).coalesce(num_partitions)

    # since we have to determine the probability of rating r for each user and item, 
    # we have to create a RDD with [(rating, (user, item))] for each rating
    # ie. [(rating_1, (user, item)), (rating_2, (user, item)), (rating_3, (user, item)), ..., (rating_5, (user, item))]
    # do not combine rCombo_ui into uirCombo since rCombo_ui will be used later on
    rCombo_ui = ui_combo.flatMap(lambda u_i: [(float(r), (u_i[0], u_i[1])) for r in range_of_ratings.value]).coalesce(num_partitions)
    uirCombo = rCombo_ui.map(lambda r_u_i: (r_u_i[1][0], r_u_i[1][1], r_u_i[0]))

    """
    Calculate P(r|u), probability of rating r for user u.
    P(r|u) = (number of ratings r that user u gives) / (total number of ratings that user u gives)

    For example, if rating r == 1, then
    P(r|u) = (number of ratings r == 1 that user u gives) / (total number of ratings that user u gives)
    """

    # For each user-rating pair, make sure that if the user never rates a certain rating r, 
    # we will set the count number as 0; otherwise for each rating that the user gives, set the count number as 1.
    # We will add the number of ratings for each rating r so that 
    # we will know the number of ratings r that user u gives.
    # [((user_id, rating), 1)]
    ur_1 = training_data.map(lambda u_i_r26: ((u_i_r26[0],u_i_r26[2]), 1))
    # [(((user_id, rating_1), 0), ((user_id, rating_2), 0), ..., ((user_id, rating_5), 0))]
    urCombo_0 = uirCombo.map(lambda u_i_r27: ((u_i_r27[0],u_i_r27[2]), 0)).distinct()
    ur_1Or0 = ur_1.union(urCombo_0)
    # [(user_id, rating), (num_rating)]
    ur_numRating = ur_1Or0.reduceByKey(add)
    # [(user_id, (rating, num_rating))]
    u_r_numRating = ur_numRating.map(lambda u_r_num_r: (u_r_num_r[0][0], (u_r_num_r[0][1], u_r_num_r[1])))
    # [(user_id, total_rating)]
    u_totalRating = sc.parallelize(list(training_data.map(lambda u_i_r: (u_i_r[0],u_i_r[2])).countByKey().items()))
    # [(user_id, (total_rating, (rating, num_rating)))]
    u_componentsOfProb = u_totalRating.join(u_r_numRating)
    # [(user_id, rating, probRU)]
    probRU = u_componentsOfProb.map(lambda u_total_r_r_num_r: (u_total_r_r_num_r[0], u_total_r_r_num_r[1][0], float(u_total_r_r_num_r[1][1])/float(u_total_r_r_num_r[1][0])))

    """
    Calculate P(r|i), probability of rating r for item i.
    P(r|i) = (number of ratings r that item i receives) / (total number of ratings that item i receives)

    For example, if rating r == 1, then
    P(r|i) = (number of ratings r == 1 that item i receives) / (total number of ratings that item i receives)
    """

    # For each item-rating pair, make sure that if the item never receives a certain rating r, 
    # we will set the count number as 0; otherwise for each rating that the item receives, set the count number as 1.
    # We will add the number of ratings for each rating r so that 
    # we will know the number of ratings r that item i receives.
    # [((item_id, rating), 1)]
    ir_1 = training_data.map(lambda u_i_r28: ((u_i_r28[1],u_i_r28[2]), 1))
    # [(((item_id, rating_1), 0), ((item_id, rating_2), 0), ..., ((item_id, rating_5), 0))]
    irCombo_0 = uirCombo.map(lambda u_i_r29: ((u_i_r29[1],u_i_r29[2]), 0)).distinct()
    ir_1Or0 = ir_1.union(irCombo_0)
    # [(item_id, rating), (num_rating)]
    ir_numRating = ir_1Or0.reduceByKey(add)
    # [(item_id, (rating, num_rating))]
    i_r_numRating = ir_numRating.map(lambda i_r_num_r: (i_r_num_r[0][0], (i_r_num_r[0][1], i_r_num_r[1])))
    # [(item_id, total_rating)]
    i_totalRating = sc.parallelize(list(training_data.map(lambda u_i_r1: (u_i_r1[1],u_i_r1[2])).countByKey().items()))
    # [(user_id, (total_rating, (rating, num_rating)))]
    i_componentsOfProb = i_totalRating.join(i_r_numRating)
    # [(item_id, rating, probRI)]
    probRI = i_componentsOfProb.map(lambda i_total_r_r_num_r: (i_total_r_r_num_r[0], i_total_r_r_num_r[1][0], float(i_total_r_r_num_r[1][1])/float(i_total_r_r_num_r[1][0])))

    """
    Calculate P(r), probability of rating r
    P(r) = (number of rating r) / (total number of ratings)

    For example, if rating == 1, then
    P(r) = (number of rating == 1) / (total number of ratings)
    """

    totalRatings = training_data.count()
    # [(rating, 1)]
    r_1 = training_data.map(lambda u_i_r30: (u_i_r30[2], 1))
    # [(rating, num_rating)]
    r_numRating = r_1.reduceByKey(add)
    # [(rating, probR)]
    probR = r_numRating.mapValues(lambda num_rating: float(num_rating) / float(totalRatings))

    """
    Calculate P(r|a,i), naive bayes probability.
    P(r|u,i) = ( (P(r|u) * P(r|i)) / P(r) ) * ( (P(u) * P(i)) / P(u, i)) = ( (P(r|u) * P(r|i)) / P(r) ) 
    """

    # add probR to [(user_id, item_id, rating)]
    components = rCombo_ui.join(probR)

    # add probRU to [(user_id, item_id, rating, probR)]
    tmp_a = components.map(lambda r_u_i_prob_r: ((r_u_i_prob_r[0][0], r_u_i_prob_r[0]), (r_u_i_prob_r[0][1], r_u_i_prob_r[1][1])))
    tmp_b = probRU.map(lambda u_r_prob_ru: ((u_r_prob_ru[0], u_r_prob_ru[1]), u_r_prob_ru[2]))
    components = tmp_a.join(tmp_b)

    # add probRI to [(user_id, item_id, rating, probR, probRU)]
    tmp_c = components.map(lambda u_r_i_prob_r_prob_ru: ((u_r_i_prob_r_prob_ru[0][0], u_r_i_prob_r_prob_ru[0][1]), (u_r_i_prob_r_prob_ru[0][0], u_r_i_prob_r_prob_ru[0][1], u_r_i_prob_r_prob_ru[1][1])))
    tmp_d = probRI.map(lambda i_r_prob_ri: ((i_r_prob_ri[0], i_r_prob_ri[1]), i_r_prob_ri[2]))
    components = tmp_c.join(tmp_d)

    # re-format
    # [((user_id, movie_id, rating), naive_bayes_probability)]
    componentsReformat = components.map(lambda i_r_u_prob_r_prob_ru_prob_ri: ((i_r_u_prob_r_prob_ru_prob_ri[0][0],i_r_u_prob_r_prob_ru_prob_ri[0][0],i_r_u_prob_r_prob_ru_prob_ri[0][1]), (i_r_u_prob_r_prob_ru_prob_ri[0][1], i_r_u_prob_r_prob_ru_prob_ri[0][2], i_r_u_prob_r_prob_ru_prob_ri[1][1])))
    bayesProb = componentsReformat.mapValues(lambda prob_r_prob_ru_prob_ri: prob_r_prob_ru_prob_ri[1] * prob_r_prob_ru_prob_ri[2] / prob_r_prob_ru_prob_ri[0])\
                                  .map(lambda u_i_r_bayes_prob: ((u_i_r_bayes_prob[0][0],u_i_r_bayes_prob[0][1]), (u_i_r_bayes_prob[0][2], u_i_r_bayes_prob[1])))

    # [((user_id, item_id), [(rating_1, bayes_prob_1), (rating_2, bayes_prob_2), ..., (rating_5, bayes_prob_5)])]
    # sort it by the lowest to the highest rating
    ui_allBayesProb = bayesProb.mapValues(lambda value: [value]).reduceByKey(lambda a, b: a + b)\
                               .mapValues(lambda value: sorted(value, key=lambda rating_bayes_prob: rating_bayes_prob[0]))

    return ui_allBayesProb

def calc_naive_bayes_map(training_data, sc, computeFromScratch=True, ui_allBayesProb=None, num_partitions=20):
    """
    Determine the predicted rating of every user-item combination using Naive Bayes MAP.
    Pai     : predicted rating for user a on item i
    P(r|a,i): Naive Bayes that computes the probability of rating r for a given user a on item i
    Pai = Argmax(r=1 to 5) P(r|a,i)

    Assumption:
    (1) Since Naive Bayes can be defined as P(r|u,i) = ( (P(r|u) * P(r|i)) / P(r) ) * ( (P(u) * P(i)) / P(u, i)), 
    we make the assumption that the latter part of the multiplication, ( (P(u) * P(i)) / P(u, i)), can be ignored.
    (2) Currently, it does not support continuous rating.

    TODO: 
        Implement this using PyMC?

    Args:
        training_data: the data used to train the RecSys algorithm in the format of a RDD of [ (userId, itemId, actualRating) ]
        sc: spark context
        computeFromScratch: option if user already called calc_naive_bayes_components() and did not want to call it again
        ui_allBayesProb: if computeFromScratch == False, this must be defined

    Returns:
        predictions: predicted ratings of every user-item combination in the format of a RDD of [(userId, itemId, predictedRating)].
    """

    def calculate_bayes_map(value):
        # value is in the format of (rating, bayes_prob)
        RATING_INDEX = 0
        BAYES_PROB_INDEX = 1
        # extract the bayes_prob 
        bayesProbList = [tup[BAYES_PROB_INDEX] for tup in value]
        # return the index of the highest bayes_prob
        argmax_index = bayesProbList.index(max(bayesProbList))
        # from the index, determine the prediction 
        argmax = value[argmax_index][RATING_INDEX]
        return argmax

    if computeFromScratch:
        ui_allBayesProb = calc_naive_bayes_components(training_data, sc)
    else:
        if ui_allBayesProb is None:
            raise Exception("ERROR: ui_allBayesProb is not defined although user specified not to calculate ui_allBayesProb from scratch.")

    return ui_allBayesProb.mapValues(calculate_bayes_map).map(lambda u_i_r31: (u_i_r31[0][0],u_i_r31[0][1],u_i_r31[1]))

def calc_naive_bayes_mse(training_data, sc, computeFromScratch=True, ui_allBayesProb=None, num_partitions=20):
    """
    Determine the predicted rating of every user-item combination using Naive Bayes MSE.
    Pai     : predicted rating for user a on item i
    P(r|a,i): Naive Bayes that computes the probability of rating r for a given user a on item i
    Pai = Sum of (r * P(r|a,i)) from r=1 to 5

    Assumption:
    (1) Since Naive Bayes can be defined as P(r|u,i) = ( (P(r|u) * P(r|i)) / P(r) ) * ( (P(u) * P(i)) / P(u, i)), 
    we make the assumption that the latter part of the multiplication, ( (P(u) * P(i)) / P(u, i)), can be ignored.
    (2) Currently, it does not support continuous rating.

    TODO: 
        Implement this using PyMC?

    Args:
        training_data: the data used to train the RecSys algorithm in the format of a RDD of [ (userId, itemId, actualRating) ]
        sc: spark context
        computeFromScratch: option if user already called calc_naive_bayes_components() and did not want to call it again
        ui_allBayesProb: if computeFromScratch == False, this must be defined

    Returns:
        predictions: predicted ratings of every user-item combination in the format of a RDD of [(userId, itemId, predictedRating)].
    """

    def calculate_bayes_mse(value):
        predicted = 0.
        for rating, bayes_prob in value:
            predicted += rating * bayes_prob
        return predicted

    if computeFromScratch:
        ui_allBayesProb = calc_naive_bayes_components(training_data, sc)
    else:
        if ui_allBayesProb is None:
            raise Exception("ERROR: ui_allBayesProb is not defined although user specified not to calculate ui_allBayesProb from scratch.")

    return ui_allBayesProb.mapValues(calculate_bayes_mse).map(lambda u_i_r32: (u_i_r32[0][0],u_i_r32[0][1],u_i_r32[1]))

def calc_naive_bayes_mae(training_data, sc, computeFromScratch=True, ui_allBayesProb=None, num_partitions=20):
    """
    Determine the predicted rating of every user-item combination using Naive Bayes MAE.
    Pai     : predicted rating for user a on item i
    P(r|a,i): Naive Bayes that computes the probability of rating r for a given user a on item i
    Pai = Argmin from r=1 to 5(Sum of (P(n|a,i) * |r-n|) from n=1 to 5)

    Assumption:
    (1) Since Naive Bayes can be defined as P(r|u,i) = ( (P(r|u) * P(r|i)) / P(r) ) * ( (P(u) * P(i)) / P(u, i)), 
    we make the assumption that the latter part of the multiplication, ( (P(u) * P(i)) / P(u, i)), can be ignored.
    (2) Currently, it does not support continuous rating.

    TODO: 
        Implement this using PyMC?

    Args:
        training_data: the data used to train the RecSys algorithm in the format of a RDD of [ (userId, itemId, actualRating) ]
        sc: spark context
        computeFromScratch: option if user already called calc_naive_bayes_components() and did not want to call it again
        ui_allBayesProb: if computeFromScratch == False, this must be defined

    Returns:
        predictions: predicted ratings of every user-item combination in the format of a RDD of [(userId, itemId, predictedRating)].
    """

    def calculate_bayes_mae(value):
        # value is in the format of (rating, bayes_prob)
        RATING_INDEX = 0
        # determine the minimum and maximum rating
        ratingList = [tup[RATING_INDEX] for tup in value]
        min_rating_index = ratingList.index(min(ratingList))
        min_rating = int(value[min_rating_index][RATING_INDEX])
        max_rating_index = ratingList.index(max(ratingList))
        max_rating = int(value[max_rating_index][RATING_INDEX])
        # calculate the sum of the product
        sumOfProductList = []
        for rating, bayes_prob in value:
            sumOfProduct = 0.
            for i in range(min_rating, max_rating+1):
                sumOfProduct += bayes_prob * abs(rating - i)
            sumOfProductList.append(sumOfProduct)
        # from the index, determine the prediction
        argmin_index = sumOfProductList.index(min(sumOfProductList))
        argmin = value[argmin_index][RATING_INDEX]
        return argmin

    if computeFromScratch:
        ui_allBayesProb = calc_naive_bayes_components(training_data, sc)
    else:
        if ui_allBayesProb is None:
            raise Exception("ERROR: ui_allBayesProb is not defined although user specified not to calculate ui_allBayesProb from scratch.")

    return ui_allBayesProb.mapValues(calculate_bayes_mae).map(lambda u_i_r33: (u_i_r33[0][0],u_i_r33[0][1],u_i_r33[1]))

