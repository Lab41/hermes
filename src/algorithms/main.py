#!/usr/bin/env python

import itertools
import json

from pyspark import SparkConf
from pyspark.mllib.recommendation import ALS

# TODO: remove these
from sklearn import datasets, svm, metrics
from sklearn.cross_validation import train_test_split
import matplotlib.pyplot as plt

import data
import collaborative_filtering as cf
import content_based as cb
from singleton import SCSingleton

def testSimpleRMSE():

	# load the data, an RDD of [(userId, itemId, rating)]
	# For testing simple RMSE, we are going to use the following given data.

	# split data into train (60%), validation (20%), test(20%)
	"""
	rating ==  1, user likes the item
	rating == -1, user dislikes the item

                     0	 1	 2	 3  = itemID
    userId =    0	 1	-1	 1	 1
                1		 1	-1	-1
                2	 1	 1	-1	
                3	-1		 1	
                4	 1	 1	 	-1

	0:  (0, 0, 1)	x
	1:  (0, 1, -1)	x
	2:  (0, 2, 1)	x
	3:  (0, 3, 1)	x
	4:  (1, 1, 1)	x
	5:  (1, 2, -1)	x
	6:  (1, 3, -1)	x
	7:  (2, 0, 1)	x
	8:  (2, 1, 1)	x
	9:  (2, 1, -1)	x
	10: (3, 0, -1)	x
	11: (3, 2, 1)	x
	12: (4, 0, 1)	x
	13: (4, 1, 1)	x
	14: (4, 3, -1)	x

	training (8): data to train the model
	validation (3):  best performing approach using the validation data
	test (3): estimate accuracy of the selected approach

	splitting this data into training, validation, test is randomly selected
	"""

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

	# train the model
	isExplicit = True 
	ranks = [3, 5, 7]
	#numIters = [5]         # default value
	#lmbdas = [0.01]        # default value
	#blocks = -1            # default value
	#nonnegative = False    # default value
	#seed = None            # default value
	model = None

	validationArray = validationRDD.collect()
	bestModel = None
	bestValidationRmse = float("inf")
	bestRank = 0

	# with validation
	#for rank, numIter, lmbda in itertools.product(ranks, numIters, lmbdas):
	for rank in ranks:
		if isExplicit:
			model = ALS.train(trainingRDD, rank)
		else: 
			model = ALS.trainImplicit(trainingRDD, rank)
		validationPredRDD = model.predictAll( validationRDD.map( lambda x: (x[0], x[1]) ) )
		validationPredArray = validationPredRDD.collect()
		validationRmse = cf.calculate_rmse(validationArray, validationPredArray)
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
	testArray = testRDD.collect()
	testPredArray = testPredRDD.collect()
	print "testArray", testArray
	print "testPredArray", testPredArray

	testRmse = cf.calculate_rmse(testArray, testPredArray)
	print "testRmse", testRmse

	return

def testRMSE():

	# load the data, an RDD of [(userId, itemId, rating)]
	# TODO

	# split data into train (60%), validation (20%), test(20%)
	# TODO: how to split the RDD optimally
	# http://datascience.stackexchange.com/questions/5667/spark-optimally-splitting-a-single-rdd-into-two

	return

def testSimpleBinaryPRFS():

	# load the data
	digits = datasets.load_digits()
	data = digits.data
	labels = digits.target

	print "numData = ", len(digits.data)
	print "numTarget = ", len(digits.target)

	# split data into train (60%), validation (20%), test(20%)
	trainingData, testData, trainingLabel, testLabel = train_test_split(data, labels, test_size=0.4)

	print "numTrainingData  = ", len(trainingData)
	print "numTestData = ", len(testData)
	print "numTrainingData == numTestData: ", (len(trainingData) == len(testData))
	print "numTrainingLabel = ", len(trainingLabel)
	print "numTestLabel == ", len(testLabel)
	print "numTrainingLabel == numTestLabel: ", (len(trainingLabel) == len(testLabel))

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

	p, r, f, s = cf.calculate_prfs(testLabel, testPredLabel)
	print "precision = ", p
	print "recall = ", r
	print "fscore = ", f
	print "support = ", s

	print "classification report for classifier model %s:\n%s\n" % \
		(model, cf.calculate_prfs_in_report(testLabel, testPredLabel))

	print "confusion matrix:\n%s" % cf.get_confusion_matrix(testLabel, testPredLabel)

	return

def testSimpleMultiPRFS():
	return


if __name__ == "__main__":

	# set up spark environment
	conf = SparkConf().setAppName("testSimpleRMSE").set("spark.executor.memory", "2g")
	scsingleton = SCSingleton(conf)

	testSimpleRMSE()
	testRMSE()

	testSimpleBinaryPRFS()
	testSimpleMultiPRFS()
