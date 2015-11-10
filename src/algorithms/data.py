#!/usr/bin/env python

class Data(object):
    def __init__(self, data, rsType=None):
        self.data = data
        self.rsType = rsType

    def getData(self):
        return self.data

    def getRSType(self):
        return self.rsType


class CFData(Data):
    """
    1. cf_vector = [(userId, itemId, actual_rating)]
    2. cf_vector = [(userId, itemId, interaction_value)]
    """
    
    def __init__(self, data, isDataTypeExplicit=True):
        Data.__init__(self, data, "CFData")
        self.isDataTypeExplicit = isDataTypeExplicit

    def isDataTypeExplicit(self):
        return self.isDataTypeExplicit

    def isDataTypeImplicit(self):
        return not self.isDataTypeExplicit

    def getDataType(self):
        if isDataTypeExplicit:
            return "explicit"
        else:
            return "implicit"


class CBData(Data):
    """
    1. cb_vector = [(itemId, itemTypeArray)] where itemTypeArray = [itemType1, ..., itemTypeN]
    """

    def __init__(self, data):
        Data.__init__(self, data, "CBData")




