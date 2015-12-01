#!/usr/bin/env python

import click
import itertools
import json
import os
import sys

from pyspark import SparkConf
from pyspark.mllib.recommendation import ALS
from pyspark.sql.types import StructType
from pyspark.mllib.util import MLUtils
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import MulticlassMetrics


from sklearn import datasets, svm
from sklearn.cross_validation import train_test_split
from sklearn.cross_validation import StratifiedShuffleSplit

sys.path.append("../algorithms")

import performance_metrics as pm
import content_based as cb
from singleton import SCSingleton
from timer import Timer

"""
This entire file is to provide a basic understanding of collaborative filtering
and its performance metrics. 

* test_simple_rmse() tests RMSE with ALS model and a small dataset using sklearn (meaning it uses array). 
* test_rmse() tests RMSE with ALS model and a large dataset using pyspark (meaning it uses RDD).
* test_simple_prfs() tests precision and recall with SVD model and a small dataset using sklearn (meaning it uses array).
* test_prfs() tests precision and recall with LogisticRegressionWithLBFGS model and a large dataset using pyspark (meaning it uses RDD).

This example assumes that you have installed 
* pyspark
* psutil
* scala
* spark
* hadoop

"""

def test_simple_rmse():
    """ Test RMSE as follows:
        (1) train the ALS model with a subset of 15 values 
        (2) predict a subset of 15 values using the trained model 
        (3) calculate RMSE or how accurately the prediction is 
            in comparison to the known values

    Values used to train the ALS model are based on a fictitious world where 
    5 users rate 4 items whether they like or dislike an item. If the user liked
    the item, he will provide a rating of 1; otherwise, if the user disliked the
    item, he will provide a rating of -1. No rating means that the user has not
    rated the item. This data will be formatted in an RDD of [(userId, itemId, rating)].
    Splitting these 15 values into training, validation, and test dataset is 
    randomly selected.

                     0   1   2   3  = itemID
    userId =    0    1  -1   1   1
                1        1  -1  -1
                2    1   1  -1  
                3   -1       1  
                4    1   1      -1

    0:  (0, 0, 1)   
    1:  (0, 1, -1)  
    2:  (0, 2, 1)   
    3:  (0, 3, 1)   
    4:  (1, 1, 1)   
    5:  (1, 2, -1)  
    6:  (1, 3, -1)  
    7:  (2, 0, 1)   
    8:  (2, 1, 1)   
    9:  (2, 1, -1)  
    10: (3, 0, -1)  
    11: (3, 2, 1)   
    12: (4, 0, 1)   
    13: (4, 1, 1)   
    14: (4, 3, -1)

    """

    # load the data, an RDD of [(userId, itemId, rating)]
    # split data into train (60%), validation (20%), test(20%)
    # training (8): data to train the model
    # validation (3):  best performing approach using the validation data
    # test (3): estimate accuracy of the selected approach
    # TODO: possible split using sklearn's train_test_split?

    trainingArray = [(4, 3, -1), (1, 1, 1), (3, 0, -1), 
                     (4, 0, 1), (1, 2, -1), (0, 0, 1), 
                     (2, 1, -1), (0, 2, 1), (1, 3, -1)]
    validationArray = [(4, 1, 1), (3, 2, 1), (2, 1, 1)]
    testArray = [(2, 0, 1), (0, 1, -1), (0, 3, 1)]

    trainingRDD = scsingleton.sc.parallelize(trainingArray)
    validationRDD = scsingleton.sc.parallelize(validationArray)
    testRDD = scsingleton.sc.parallelize(testArray)

    numTraining = trainingRDD.count()
    numValidation = validationRDD.count()
    numTest = testRDD.count()

    print "numTraining = %d\n" % (numTraining)
    print "numValidation = %d\n" % (numValidation)
    print "numTest = %d\n" % (numTest)

    # run training algorithm to build the model 
    isExplicit = True 
    ranks = [3, 5, 7]
    #numIters = [5]         # default value
    #lmbdas = [0.01]        # default value
    #blocks = -1            # default value
    #nonnegative = False    # default value
    #seed = None            # default value
    #alpha = [0.01]         # default value
    model = None

    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0

    # with validation
    #for rank, numIter, lmbda in itertools.product(ranks, numIters, lmbdas):
    for rank in ranks:
        if isExplicit:
            model = ALS.train(trainingRDD, rank)
        else: 
            # TODO: figure out why trainImplicit crash
            model = ALS.trainImplicit(trainingRDD, rank, iterations=5, alpha=0.01)
        validationPredRDD = model.predictAll( validationRDD.map( lambda x: (x[0], x[1]) ) )
        validationRmse = pm.calculate_rmse_using_rdd(validationRDD, validationPredRDD, numValidation)
        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank

    # make a prediction
    testPredRDD = bestModel.predictAll( testRDD.map( lambda x: (x[0], x[1]) ) ).cache()

    """
    # without validation
    model = ALS.train(trainingRDD, rank=3)
    testPredRDD = model.predictAll( testRDD.map( lambda x: (x[0], x[1]) ) )
    """

    # calculate RMSE
    testRmse = pm.calculate_rmse_using_rdd(testRDD, testPredRDD, numTest)
    print "testRmse using RDD = ", testRmse

    return

def test_rmse():
    # TODO: revised so that it will take user's inputs instead of hardcoded values

    movies_schema = None
    ratings_schema = None

    # load the schemas
    with open("/Users/tiffanyj/datasets/movielens/movielens_20m_movies_schema.json", "r") as json_schema_file:
        movies_schema = StructType.fromJson(json.load(json_schema_file))

    with open("/Users/tiffanyj/datasets/movielens/movielens_20m_ratings_schema.json", "r") as json_schema_file:
        ratings_schema = StructType.fromJson(json.load(json_schema_file))

    # create a hdfs directory
    os.system("hdfs dfs -mkdir /user/tiffanyj/datasets")

    # load the json file into the hdfs directory
    os.system("hdfs dfs -put /Users/tiffanyj/datasets/movielens/movielens_10m_ratings.json.gz /user/tiffanyj/datasets/movielens_10m_ratings.json.gz")

    # create a DataFrame based on the content of the json file
    ratingsDF = scsingleton.sqlCtx.read.json("hdfs://localhost:9000/user/tiffanyj/datasets/movielens_10m_ratings.json.gz", schema=ratings_schema)
    # explicitly repartition RDD after loading so that more tasks can run on it in parallel
    # by default, defaultMinPartitions == defaultParallelism == estimated # of cores across all of the machines in your cluster
    ratingsDF = ratingsDF.repartition(scsingleton.sc.defaultParallelism * 3)    

    # parse ratings DataFrame into an RDD of [(userId, itemId, rating)]
    ratingsRDD = ratingsDF.map(lambda row: (row.user_id, row.movie_id, row.rating))
    ratingsRDD.cache()

    # split data into train (60%), test (40%)
    # TODO: add validation in the future? train (60%), validation (20%), test(20%)?
    trainingRDD, testRDD = ratingsRDD.randomSplit([0.6, 0.4])
    trainingRDD.cache()
    testRDD.cache()

    with Timer() as t:
        numTest = testRDD.count()
    print "testRDD.count(): %s seconds" % t.secs

    # run training algorithm to build the model
    # without validation
    with Timer() as t:
        model = ALS.train(trainingRDD, rank=3)
    print "ALS.train(trainingRDD, rank=3): %s seconds" % t.secs

    # make a prediction
    with Timer() as t:
        testPredRDD = model.predictAll( testRDD.map( lambda x: (x[0], x[1]) ) ).cache()
    print "testPredRDD: %s seconds" % t.secs

    # calculate RMSE
    with Timer() as t:
        testRmse = pm.calculate_rmse_using_rdd(testRDD, testPredRDD, numTest)
    print "testRmse: %s seconds" % t.secs
    print "testRmse", testRmse

    return

def test_simple_prfs():
    """ Test Precision and Recall at N (as well as F1-score and Support) as follows:
        (1) train the SVC model with a subset of sklearn's digits dataset
        (2) predict what the number is using 
            the trained model and a subset of sklearn's digits dataset
        (3) calculate "Precision and Recall at N" or how accurately it classifies the
            digit in comparison to the known values

    """

    # load the data
    digits = datasets.load_digits()
    data = digits.data
    labels = digits.target

    #print "data\n", data[0]
    #print "labels\n", labels
    print "numData = ", len(digits.data)
    print "numTarget = ", len(digits.target)

    # split data into train (60%), test(40%)
    # TODO: add validation in the future? train (60%), validation (20%), test(20%)?
    trainingData, testData, trainingLabel, testLabel = train_test_split(data, labels, test_size=0.4)

    print "numTrainingData  = ", len(trainingData)
    print "numTestData = ", len(testData)
    print "numTrainingLabel = ", len(trainingLabel)
    print "numTestLabel == ", len(testLabel)

    # train the model
    model = svm.SVC(gamma=0.001, C=100)
    model.fit(trainingData, trainingLabel)

    # make a prediction
    testPredLabel = model.predict(testData)

    # calculate PRFS
    print "testLabel"
    print testLabel
    print "testPredictedLabel"
    print testPredLabel
 
    p, r, f, s = pm.calculate_prfs_using_array(testLabel, testPredLabel)
    print "precision =\n", p
    print "recall =\n", r
    print "fscore =\n", f
    print "support =\n", s

    return

def test_prfs():
    # TODO: revised so that it will take user's inputs instead of hardcoded values
    
    """
    Test Precision, Recall, Fscore, and Support on multiclass classification data
    Input data: https://github.com/apache/spark/blob/master/data/mllib/sample_multiclass_classification_data.txt.
    """

    # load the schemas (if existed)

    # create a hdfs directory
    #os.system("hdfs dfs -mkdir user/tiffanyj/datasets")

    # load the data file into the hdfs directory
    os.system("hdfs dfs -put /Users/tiffanyj/datasets/sample_multiclass_classification_data.txt /user/tiffanyj/datasets/sample_multiclass_classification_data.txt")
    data = MLUtils.loadLibSVMFile(scsingleton.sc, "hdfs://localhost:9000/user/tiffanyj/datasets/sample_multiclass_classification_data.txt")
   
    # print data.take(1)
    # ie. [LabeledPoint(1.0, (4,[0,1,2,3],[-0.222222,0.5,-0.762712,-0.833333]))] 
    # [ ( finalClassification, (numLabels, [label0, label1, label2, ..., labelN], [prob0, prob1, prob2, ..., probN]) ) ]

    # split data into train (60%), test (40%)
    trainingRDD, testRDD = data.randomSplit([0.6, 0.4])
    trainingRDD.cache()
    testRDD.cache()

    with Timer() as t:
        numTest = testRDD.count()
    print "testRDD.count(): %s seconds" % t.secs

    # run training algorithm to build the model
    # without validation
    with Timer() as t:
        model = LogisticRegressionWithLBFGS.train(trainingRDD, numClasses=3)
    print "LogisticRegressionWithLBFGS.train(trainingRDD, numClasses=3): %s seconds" % t.secs

    # make a prediction
    with Timer() as t:
        testPredAndLabel = testRDD.map(lambda lp: (float(model.predict(lp.features)), lp.label))
    print "testPredAndLabel: %s seconds" % t.secs

    # calculate Precision, Recall, F1-score
    metrics = MulticlassMetrics(testPredAndLabel)
    print( "precision = %s" % metrics.precision() )
    print( "recall = %s" % metrics.recall() )
    print( "f1-score = %s" % metrics.fMeasure() )

    # statistics by class
    labels = data.map(lambda lp: lp.label).distinct().collect()
    for label in sorted(labels):
        print( "Class %s precision = %s" % (label, metrics.precision(label)) )
        print( "Class %s recall = %s" % (label, metrics.recall(label)) )
        print( "Class %s f1-score = %s" % (label, metrics.fMeasure(label, beta=1.0)) )

    # weighted stats
    print( "Weighted precision = %s" % metrics.weightedPrecision )
    print( "Weighted recall = %s" % metrics.weightedRecall )
    print( "Weighted f1-score = %s" % metrics.weightedFMeasure() )
    print( "Weighted f(0.5)-score = %s" % metrics.weightedFMeasure(beta=0.5) )
    print( "Weighted false positive rate = %s" % metrics.weightedFalsePositiveRate )
    
    return

if __name__ == "__main__":

    # set up spark environment
    conf = SparkConf().setAppName("test_precision_metrics").set("spark.executor.memory", "5g")
    scsingleton = SCSingleton(conf)

    test_simple_rmse()
    test_rmse()

    test_simple_prfs()
    test_prfs()
