#!/usr/bin/env python

from math import sqrt
from sklearn.metrics import classification_report
from sklearn.metrics import mean_squared_error
from sklearn.metrics import precision_recall_fscore_support

# Accuracy of ratings predictions ==============================================

# RMSE

def calculate_rmse(y_actual, y_predicted):
    return sqrt(mean_squared_error(y_actual, y_predicted))
    #return mean_squared_error(y_actual, y_predicted) ** 0.5

# Accuracy of usage predictions ================================================

# Performance, Recall, Fbeta Score, Support

def calculate_prfs(y_actual, y_predicted):
    """
    3rd param: labels = [-1, 0, +1]
    4th param: average = 'macro' / 'micro' / 'weighted'
    """
    return precision_recall_fscore_support(y_actual, y_predicted)

def calculate_prfs_in_report(y_actual, y_predicted):
    """
    3rd param: target_names=labels = [-1, 0, 1]
    """
    return classification_report(y_actual, y_predicted) 

# Accuracy of rankings of items ================================================

# TODO

# ==============================================================================
