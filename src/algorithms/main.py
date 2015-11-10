#!/usr/bin/env python

import itertools
import json

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

import data
import collaborative_filtering as cf
import content_based as cb

def testRMSE():

	# set up spark environment
	conf = SparkConf().setAppName("testRMSE").set("spark.executor.memory", "2g")
	sc = SparkContext(conf=conf)

	# load the data, an RDD of [(userId, itemId, rating)]
	# TODO

	# split data into train (60%), validation (20%), test(20%)
	"""
		 0	 1	 2	 3 
	0	 1	-1	 1	 1
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

	training (8): 
	validation (3):  best performing approach using the validation data
	test (3): estimate accuracy of the selected approach
	"""

	trainingArray = [(4, 3, -1), (1, 1, 1), (3, 0, -1), 
					 (4, 0, 1), (1, 2, -1), (0, 0, 1), 
					 (2, 1, -1), (0, 2, 1), (1, 3, -1)]
	validationArray = [(4, 1, 1), (3, 2, 1), (2, 1, 1)]
	testArray = [(2, 0, 1), (0, 1, -1), (0, 3, 1)]

	trainingRDD = sc.parallelize(trainingArray)
	validationRDD = sc.parallelize(validationArray)
	testRDD = sc.parallelize(testArray)

	numTraining = trainingRDD.count()
	numValidation = validationRDD.count()
	numTest = testRDD.count()

	print "numTraining = %d\n" % (numTraining)
	print "numValidation = %d\n" % (numValidation)
	print "numTest = %d\n" % (numTest)

	# train the explicit model 
	ranks = [3, 5, 7]
	"""
	# default values
	numIters = [5]
	lmbdas = [0.01]
	blocks = -1
	nonnegative = False
	seed = None
	"""
	model = ALS.train(trainingRDD, rank=3)
	testPredRDD = model.predictAll( testRDD.map( lambda x: (x[0], x[1]) ) )

	testArray = testRDD.collect()
	testPredArray = testPredRDD.collect()
	print "testArray", testArray
	print "testPredArray", testPredArray

	testRmse = cf.calculate_rmse(testArray, testPredArray)
	print "testRmse", testRmse

	# make personalized recommendation
	predictMeArray = [(1, 1), (2, 3), (3, 1), (3, 3), (4, 2)]
	predictMeRDD = sc.parallelize(predictMeArray)
	prediction = model.predictAll(predictMeRDD).collect()
	print prediction

	"""
	# with validation
	bestModel = None
	bestValidationRmse = float("inf")
	bestRank = 0

	for rank in itertools.product(ranks):
		model = ALS.train(training, rank)
		validationRmse = computeRmse(model, validation, numValidation)
		print "RMSE (validation) = %f for the model tarined with " % (validationRmse) + \
		      "rank = %d" % (rank)
		if(validationRmse < bestValidationRmse):
			bestModel = model
			bestValidationRmse = validationRmse
			bestRank = rank

	"""

	return

def testPRFS():
	return


if __name__ == "__main__":
	testRMSE()
	testPRFS()
