import numpy as np

class osm_vectorize():

    def __init__(self, user_interactions, user_vector_type, content_vector_type, sqlCtx, **support_files ):
        """
        Class initializer to load the required files

        Args:
            user_interactions: The raw RDD of the user interactions. For OSM, these are the object edits as well as the object data
            user_vector_type: The type of user vector desired.  For MovieLens you can choose between ['ratings', 'pos_ratings', 'ratings_to_interact', 'none'].
                If 'none' is used then this means you will run your own custom mapping
            content_vector_type: The type of content vector desired. For MovieLens you can choose between ['tags_only', 'none'].
                If none is chosen no content vector will be returned and None may be passed into the content argument.
                You do not need a content vector to run pure CF only but some performance metrics will not be able to be ran
            support_files: If they exist, the supporting files, dataFrames, and/or file links necessary to run the content vectors.


        """
        self.user_vector_type = user_vector_type
        self.content_vector_type = content_vector_type
        self.sqlCtx = sqlCtx

        #Filter out uninteresting items and users if they still exist in the dataset
        self.user_interactions =user_interactions
        self.user_interactions.registerTempTable("osm_data")

        filtered =  self.sqlCtx.sql("select * from osm_data where id is not Null and uid is not Null")
        filtered.registerTempTable("filtered_osm")

        #if no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}


    def get_user_vector(self):

        if self.user_vector_type=='ratings':
            user_info = self.sqlCtx.sql("select uid, id, count(1) as rating from filtered_osm group by uid, id")\
                .map(lambda (user, item, interact):(int(user), int(item), interact))
            return user_info

        elif self.user_vector_type=='any_interact':
            user_info = self.sqlCtx.sql("select uid, id, 1 as rating from filtered_osm group by uid, id")\
                .map(lambda (user, item, interact):(int(user), int(item), interact))
            return user_info

        elif self.user_vector_type=='num_edits_ceil':
            user_info = self.sqlCtx.sql("select uid, id, count(1) as rating from filtered_osm group by uid, id") \
                                   .map(lambda (user, item, interact) : (user, int(item), min(interact, 5)))
            return user_info

        elif self.user_vector_type=='none':
            return None

        else:
            print "Please choose a user_vector_type between 'ratings', 'any_interact', 'num_edits_ceil', and 'none'"
            return None

    def get_content_vector(self):

        if self.content_vector_type=='tags_only':
            content_array = self.content.map(lambda row: (row.id, osm_vectorize(row)))\
                .groupByKey().map(lambda (id, vectors): (id, np.array(list(vectors)).max(axis=0)))
            return content_array

        elif self.content_vector_type=='none':
            return None

        else:
            print "Please choose a content_vector_type between 'tags_only' or 'none'"
            return None




def osm_vectorize(row):
    vect = []
    if row.source is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.building is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.highway is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.name is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.addr_city is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.addr_postcode is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.natural is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.landuse is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.surface is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.waterway is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.power is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.wall is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.oneway is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.amenity is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.ref is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.building_levels is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.maxspeed is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.barrier is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.type is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.place is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.foot is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.bicycle is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.railway is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.leisure is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.bridge is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.parking is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.man_made is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.railway is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.aeroway is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.wikipedia is not None:
        vect.append(1)
    else:
        vect.append(0)
    if row.osm_type =='Node':
        vect.append(1)
    else:
        vect.append(0)
    if row.osm_type =='Way':
        vect.append(1)
    else:
        vect.append(0)
    if row.osm_type =='Relation':
        vect.append(1)
    else:
        vect.append(0)
    return vect
