#!/usr/bin/env python

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

class SCSingleton(object):
    __instance = None

    def __new__(cls, conf):
        if SCSingleton.__instance is None:
            SCSingleton.__instance = object.__new__(cls)
            SCSingleton.__instance.conf = conf
            SCSingleton.__instance.sc = SparkContext(conf=conf)
            SCSingleton.__instance.sqlCtx = SQLContext(SCSingleton.__instance.sc)
        else:
            print "ERROR: An instance of a SparkContext is already created. " + \
                          " That instance is ", SCSingleton.__instance.conf
        return SCSingleton.__instance