import numpy as np
import recommender_helpers as rechelp
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
    max_rating = y_training_data.map(lambda (user, item, rating): rating).max()
    min_rating = y_training_data.map(lambda (user, item, rating): rating).min()

    if max_rating == min_rating:
        min_rating=0

    #MLLIb has two methods, train and trainImplicit().  Implicit data will go between zero and 1
    if min_rating==0 and max_rating==1:
        model = ALS.trainImplicit(y_training_data, rank = 10, iterations = 5)
    else:
        model = ALS.train(y_training_data, rank = 10, iterations = 5)

    #predict all user, item pairs
    item_ids = y_training_data.map(lambda (u,i,r): i).distinct()
    user_ids = y_training_data.map(lambda (u,i,r): u).distinct()
    user_item_combo = user_ids.cartesian(item_ids).coalesce(num_partitions)

    predicted = model.predictAll(user_item_combo.map(lambda x: (x[0], x[1])))

    norm_predictions = predicted.map(lambda (user,item,pred): (user,item, rechelp.squish_preds(pred,min_rating,max_rating)))


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

    user_groups = training_data.groupBy(lambda (user, item, rating): user)

    user_groups_sim = user_groups.cartesian(user_groups).map(lambda ((user1_id, user1_rows), (user2_id, user2_rows)):\
        (user1_id, user2_id, similarity(user1_rows, user2_rows, 1))).coalesce(num_partitions)
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

    user_groups = training_data.groupBy(lambda (user, item, rating): user)

    user_groups_sim = user_groups.cartesian(user_groups).map(lambda ((user1_id, user1_rows), (user2_id, user2_rows)):\
        (user1_id, user2_id, similarity(user1_rows, user2_rows, 1))).coalesce(num_partitions)

    user_averages = training_data.map(lambda (user, item, rating): (user, (rating))).groupByKey().\
        map(lambda (user, ratings): (user, np.mean(list(ratings))))

    user_resids = training_data.map(lambda (user, item, rating): (user, (item, rating))).join(user_averages)\
        .map(lambda (user, ((item, rating), avg_rating)): (user, (item, rating-avg_rating)))

    item_adjustments = user_resids.join(user_groups_sim.map(lambda (u1, u2, sim): (u1, (u2, sim))))\
        .map(lambda (u1, ((item, resid), (u2, sim))): ((u2,item), (resid*sim, sim))).\
        groupByKey().map(lambda ((user, item), sim_list): (user, item, calc_item_adjust(sim_list)))

    predictions = item_adjustments.map(lambda (user, item, item_adj): (user, (item, item_adj))).join(user_averages)\
        .map(lambda (user, ((item, item_adj), (avg_rate))): (user, item, avg_rate+item_adj))

    #Predicted values can be anywhere - because we are normalizing the content based algorithms we should likely normalize here
    max_rating = training_data.map(lambda (user, item, rating): rating).max()
    min_rating = training_data.map(lambda (user, item, rating): rating).min()

    if max_rating == min_rating:
        min_rating=0

    norm_predictions = predictions.map(lambda (user,item,pred): (user,item, rechelp.squish_preds(pred,min_rating,max_rating)))

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

    item_groups = training_data.groupBy(lambda (user, item, rating): item)
    item_similarity = item_groups.cartesian(item_groups).map(lambda ((item1_id, item1_rows), (item2_id, item2_rows)):\
                       (item1_id, item2_id, similarity(item1_rows, item2_rows, 0))).coalesce(num_partitions)

    user_item_sim = training_data.keyBy(lambda (user, item, rating): item)\
        .join(item_similarity.keyBy(lambda (item1, item2, sim): item1))\
        .map(lambda (item_id,((user, item, rating),(item1, item2, sim))):((user, item2), (item,rating,sim)))\
        .filter(lambda ((user, item2), (item,rating,sim)): item2!=item)

    predictions = user_item_sim.groupByKey()\
        .map(lambda ((user, item), rows): (user, item, get_item_prob(rows)))

    #Predicted values can be anywhere - because we are normalizing the content based algorithms we should likely normalize here
    max_rating = training_data.map(lambda (user, item, rating): rating).max()
    min_rating = training_data.map(lambda (user, item, rating): rating).min()

    if max_rating == min_rating:
        min_rating=0

    norm_predictions = predictions.map(lambda (user,item,pred): (user,item, rechelp.squish_preds(pred,min_rating,max_rating)))

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
        sim = cosine_similarity(*zip(*rating_match))[0][0]

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
    r_ui_train = training_data.map(lambda (u,i,r): LabeledPoint(r, (u, i)))
    # train Naive Bayes model
    naiveBayesModel = NaiveBayes.train(r_ui_train, lambda_=1.0)
    # predict on all user-item pairs
    user_ids = training_data.map(lambda (u,i,r): u).distinct()
    item_ids = training_data.map(lambda (u,i,r): i).distinct()
    ui_combo = user_ids.cartesian(item_ids).coalesce(num_partitions)
    r_ui_combo = ui_combo.map(lambda (u,i,r): LabeledPoint(1, (u, i)))
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
    min_rating = training_data.map(lambda (u,i,r): r).min()
    max_rating = training_data.map(lambda (u,i,r): r).max()
    range_of_ratings = sc.parallelize(list(range(int(min_rating), int(max_rating + 1))))

    #predict on all user-item pairs
    user_ids = training_data.map(lambda (u,i,r): u).distinct()
    item_ids = training_data.map(lambda (u,i,r): i).distinct()
    ui_combo = user_ids.cartesian(item_ids).coalesce(num_partitions)

    # since we have to determine the probability of rating r for each user and item, 
    # we have to create a RDD with [(rating, (user, item))] for each rating
    # ie. [(rating_1, (user, item)), (rating_2, (user, item)), (rating_3, (user, item)), ..., (rating_5, (user, item))]
    rCombo_ui = range_of_ratings.cartesian(ui_combo).map(lambda (r, (u,i)): (float(r), (u, i)))
    uirCombo = rCombo_ui.map(lambda (r, (u,i)): (u,i,r))

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
    ur_1 = training_data.map(lambda (u,i,r): ((u,i), 1))
    # [(((user_id, rating_1), 0), ((user_id, rating_2), 0), ..., ((user_id, rating_5), 0))]
    urCombo_0 = uirCombo.map(lambda (u,i,r): ((u,i), 0)).distinct()
    ur_1Or0 = ur_1.union(urCombo_0)
    # [(user_id, rating), (num_rating)]
    ur_numRating = ur_1Or0.reduceByKey(add)
    # [(user_id, (rating, num_rating))]
    u_r_numRating = ur_numRating.map(lambda ((u,r), num_r): (u, (r, num_r)))
    # [(user_id, total_rating)]
    u_totalRating = sc.parallelize(training_data.map(lambda (u,i,r): (u,r)).countByKey().items())
    # [(user_id, (total_rating, (rating, num_rating)))]
    u_componentsOfProb = u_totalRating.join(u_r_numRating)
    # [(user_id, rating, probRU)]
    probRU = u_componentsOfProb.map(lambda (u, (total_r, (r, num_r))): (u, r, float(num_r)/float(total_r)))
    
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
    ir_1 = training_data.map(lambda (u,i,r): ((i,r), 1))
    # [(((item_id, rating_1), 0), ((item_id, rating_2), 0), ..., ((item_id, rating_5), 0))]
    irCombo_0 = uirCombo.map(lambda (u,i,r): ((i,r), 0)).distinct()
    ir_1Or0 = ir_1.union(irCombo_0)
    # [(item_id, rating), (num_rating)]
    ir_numRating = ir_1Or0.reduceByKey(add)
    # [(item_id, (rating, num_rating))]
    i_r_numRating = ir_numRating.map(lambda ((i,r), num_r): (i, (r, num_r)))
    # [(item_id, total_rating)]
    i_totalRating = sc.parallelize(training_data.map(lambda (u,i,r): (i,r)).countByKey().items())
    # [(user_id, (total_rating, (rating, num_rating)))]
    i_componentsOfProb = i_totalRating.join(i_r_numRating)
    # [(item_id, rating, probRI)]
    probRI = i_componentsOfProb.map(lambda (i,(total_r, (r, num_r))): (i, r, float(num_r)/float(total_r)))

    """
    Calculate P(r), probability of rating r
    P(r) = (number of rating r) / (total number of ratings)

    For example, if rating == 1, then
    P(r) = (number of rating == 1) / (total number of ratings)
    """

    totalRatings = training_data.count()
    # [(rating, 1)]
    r_1 = training_data.map(lambda (u,i,r): (r, 1))
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
    tmp_a = components.map(lambda (r, ((u,i), prob_r)): ((u, r), (i, prob_r)))
    tmp_b = probRU.map(lambda (u, r, prob_ru): ((u, r), prob_ru))
    components = tmp_a.join(tmp_b)

    # add probRI to [(user_id, item_id, rating, probR, probRU)]
    tmp_a = components.map(lambda ((u, r), ((i, prob_r), prob_ru)): ((i, r), (u, prob_r, prob_ru)))
    tmp_b = probRI.map(lambda (i, r, prob_ri): ((i, r), prob_ri))
    components = tmp_a.join(tmp_b)

    # re-format
    # [((user_id, movie_id, rating), naive_bayes_probability)]
    componentsReformat = components.map(lambda ((i, r), ((u, prob_r, prob_ru), prob_ri)): ((u,i,r), (prob_r, prob_ru, prob_ri)))
    bayesProb = componentsReformat.mapValues(lambda (prob_r, prob_ru, prob_ri): prob_ru * prob_ri / prob_r)\
                                  .map(lambda ((u,i,r), bayes_prob): ((u,i), (r, bayes_prob)))

    # [((user_id, item_id), [(rating_1, bayes_prob_1), (rating_2, bayes_prob_2), ..., (rating_5, bayes_prob_5)])]
    # sort it by the lowest to the highest rating
    ui_allBayesProb = bayesProb.mapValues(lambda value: [value]).reduceByKey(lambda a, b: a + b)\
                               .mapValues(lambda value: sorted(value, key=lambda(rating, bayes_prob): rating))

    return ui_allBayesProb

def calc_naive_bayes_map(training_data, sc, computeFromScratch=True, ui_allBayesProb=None, num_partitions=20):
    """
    Determine the predicted rating of every user-item combination using Naive Bayes MAP.
    Pai     : predicted rating for user a on item i
    P(r|a,i): Naive Bayes that computes the probability of rating r for a given user a on item i
    Pai = Argmax(r=1 to 5) P(r|a,i)

    Assumption:
    Since Naive Bayes can be defined as P(r|u,i) = ( (P(r|u) * P(r|i)) / P(r) ) * ( (P(u) * P(i)) / P(u, i)), 
    we make the assumption that the latter part of the multiplication, ( (P(u) * P(i)) / P(u, i)), can be ignored.

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
        # extract the bayes_prob 
        bayesProbList = [x[1] for x in value]
        
        # define the argmax, return the index
        argmax = bayesProbList.index(max(bayesProbList))
        
        return argmax

    if computeFromScratch:
        ui_allBayesProb = calc_naive_bayes_components(training_data, sc)
    else:
        if ui_allBayesProb is None:
            raise Exception("ERROR: ui_allBayesProb is not defined although user specified not to calculate ui_allBayesProb from scratch.")

    return ui_allBayesProb.mapValues(calculate_bayes_map).map(lambda ((u,i),r): (u,i,r))

def calc_naive_bayes_mse(training_data, sc, computeFromScratch=True, ui_allBayesProb=None, num_partitions=20):
    """
    Determine the predicted rating of every user-item combination using Naive Bayes MSE.
    Pai     : predicted rating for user a on item i
    P(r|a,i): Naive Bayes that computes the probability of rating r for a given user a on item i
    Pai = Sum of (r * P(r|a,i)) from r=1 to 5

    Assumption:
    Since Naive Bayes can be defined as P(r|u,i) = ( (P(r|u) * P(r|i)) / P(r) ) * ( (P(u) * P(i)) / P(u, i)), 
    we make the assumption that the latter part of the multiplication, ( (P(u) * P(i)) / P(u, i)), can be ignored.

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

    return ui_allBayesProb.mapValues(calculate_bayes_mse).map(lambda ((u,i),r): (u,i,r))

def calc_naive_bayes_mae(training_data, sc, computeFromScratch=True, ui_allBayesProb=None, num_partitions=20):
    """
    Determine the predicted rating of every user-item combination using Naive Bayes MAE.
    Pai     : predicted rating for user a on item i
    P(r|a,i): Naive Bayes that computes the probability of rating r for a given user a on item i

    Assumption:
    Since Naive Bayes can be defined as P(r|u,i) = ( (P(r|u) * P(r|i)) / P(r) ) * ( (P(u) * P(i)) / P(u, i)), 
    we make the assumption that the latter part of the multiplication, ( (P(u) * P(i)) / P(u, i)), can be ignored.

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
        sumOfProductList = []
        for rating, bayes_prob in value:
            sumOfProduct = 0.
            for i in range(1, 6):
                sumOfProduct += bayes_prob * abs(rating - i)
            sumOfProductList.append(sumOfProduct)
            
        argmin = sumOfProductList.index(min(sumOfProductList))

        return argmin

    if computeFromScratch:
        ui_allBayesProb = calc_naive_bayes_components(training_data, sc)
    else:
        if ui_allBayesProb is None:
            raise Exception("ERROR: ui_allBayesProb is not defined although user specified not to calculate ui_allBayesProb from scratch.")

    return ui_allBayesProb.mapValues(calculate_bayes_mae).map(lambda ((u,i),r): (u,i,r))

