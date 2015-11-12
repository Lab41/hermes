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

                     0	 1	 2	 3  = itemID
    userId =    0	 1	-1	 1	 1
                1		 1	-1	-1
                2	 1	 1	-1	
                3	-1		 1	
                4	 1	 1	 	-1

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
	#alpha = [0.01]         # default value
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
			# TODO: figure out why trainImplicit crash
			model = ALS.trainImplicit(trainingRDD, rank, iterations=5, alpha=0.01)
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

def test_rmse():

	# load the data, an RDD of [(userId, itemId, rating)]
	# TODO

	# split data into train (60%), validation (20%), test(20%)
	# TODO: how to split the RDD optimally
	# http://datascience.stackexchange.com/questions/5667/spark-optimally-splitting-a-single-rdd-into-two

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

	p, r, f, s = cf.calculate_prfs(testLabel, testPredLabel)
	print "precision =\n", p
	print "recall =\n", r
	print "fscore =\n", f
	print "support =\n", s

	print "classification report for classifier model %s:\n%s\n" % \
		(model, cf.calculate_prfs_in_report(testLabel, testPredLabel))

	print "confusion matrix:\n%s" % cf.get_confusion_matrix(testLabel, testPredLabel)

	return

def test_prfs():
	return


if __name__ == "__main__":

	# set up spark environment
	conf = SparkConf().setAppName("testSimpleRMSE").set("spark.executor.memory", "5g")
	scsingleton = SCSingleton(conf)

	test_simple_rmse()
	test_rmse()

	test_simple_prfs()
	test_prfs()
