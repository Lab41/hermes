#!/usr/bin/env python

from math import sqrt
from operator import add

# for arrays
from sklearn.metrics import classification_report
from sklearn.metrics import mean_squared_error
from sklearn.metrics import precision_recall_fscore_support
from sklearn.metrics import confusion_matrix

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

    return sqrt(sum_ratings_diff_sq) / float(num) 
        
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

    return sqrt(sum_ratings_diff) / float(num) 

# Accuracy of usage predictions (aka classification metrics) ===================

# Performance, Recall, Fbeta Score, Support

def calculate_prfs_using_rdd(y_actual, y_predicted):
    # TODO: it is highly dependent on the labels 
    return

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
