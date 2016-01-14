from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql.types import *
from pyspark.mllib.recommendation import ALS
import numpy as np

def calc_cf_mllib(y_training_data):
    """
    Utilizes the ALS collaborative filtering algorithm in MLLib to determine the predicted ratings

    Args:
        y_training_data: the data used to train the RecSys algorithm in the format of an RDD of [ (userId, itemId, actualRating) ]

    Returns:
        predicted: predicted ratings in the format of a RDD of [ (userId, itemId, predictedRating) ].

    """

    model = ALS.train(y_training_data, rank = 10, iterations = 5)
    #predict all user, item pairs
    item_ids = y_training_data.map(lambda (u,i,r): i).distinct()
    user_ids = y_training_data.map(lambda (u,i,r): u).distinct()
    user_item_combo = user_ids.cartesian(item_ids)

    predicted = model.predictAll(user_item_combo.map(lambda x: (x[0], x[1])))

    return predicted


def calc_user_user_cf(training_data):
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
        (user1_id, user2_id, similarity(user1_rows, user2_rows, 1)))
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

def calc_user_user_cf2(training_data):
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
        (user1_id, user2_id, similarity(user1_rows, user2_rows, 1)))

    user_averages = training_data.map(lambda (user, item, rating): (user, (rating))).groupByKey().\
        map(lambda (user, ratings): (user, np.mean(list(ratings))))

    user_resids = training_data.map(lambda (user, item, rating): (user, (item, rating))).join(user_averages)\
        .map(lambda (user, ((item, rating), avg_rating)): (user, (item, rating-avg_rating)))

    item_adjustments = user_resids.join(user_groups_sim.map(lambda (u1, u2, sim): (u1, (u2, sim))))\
        .map(lambda (u1, ((item, resid), (u2, sim))): ((u2,item), (resid*sim, sim))).\
        groupByKey().map(lambda ((user, item), sim_list): (user, item, calc_item_adjust(sim_list)))

    predictions = item_adjustments.map(lambda (user, item, item_adj): (user, (item, item_adj))).join(user_averages)\
        .map(lambda (user, ((item, item_adj), (avg_rate))): (user, item, avg_rate+item_adj))

    return predictions

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
        return sum_r_w/sum_sim

def calc_item_item_cf(training_data):
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
                       (item1_id, item2_id, similarity(item1_rows, item2_rows, 0)))

    user_item_sim = training_data.keyBy(lambda (user, item, rating): item)\
        .join(item_similarity.keyBy(lambda (item1, item2, sim): item1))\
        .map(lambda (item_id,((user, item, rating),(item1, item2, sim))):((user, item2), (item,rating,sim)))\
        .filter(lambda ((user, item2), (item,rating,sim)): item2!=item)

    predictions = user_item_sim.groupByKey()\
        .map(lambda ((user, item), rows): (user, item, get_item_prob(rows)))

    return predictions

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
        item_prob = nom/denom
        return float(item_prob)