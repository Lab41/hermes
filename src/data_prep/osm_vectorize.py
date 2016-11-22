import numpy as np
import shapefile

class osm_vectorize():

    def __init__(self, user_interactions, content_relations, user_vector_type, content_vector_type, sqlCtx, **support_files ):
        """
        Class initializer to load the required files

        Args:
            user_interactions: The raw RDD of the user interactions. For OSM, these are the object edits as well as the object data
            object_relations: The RDD of the content relationships.  This is used so that ways and relations inherit properties of their components
            user_vector_type: The type of user vector desired.  For OSM you can choose between ['num_edits', 'any_interact', 'num_edits_ceil', 'none'].
                If 'none' is used then this means you will run your own custom mapping
            content_vector_type: The type of content vector desired. For MovieLens you can choose between ['tags_only', tags_w_geo, 'none'].
                If none is chosen no content vector will be returned and None may be passed into the content argument.
                You do not need a content vector to run pure CF only but some performance metrics will not be able to be ran
            support_files: If they exist, the supporting files, dataFrames, and/or file links necessary to run the content vectors.


        """
        self.user_vector_type = user_vector_type
        self.content_vector_type = content_vector_type
        self.sqlCtx = sqlCtx

        self.user_interactions =user_interactions
        self.user_interactions.registerTempTable("osm_data")

        #filter out rows without an id or user id (IP address)
        self.filtered =  self.sqlCtx.sql("select * from osm_data where id is not Null and uid is not Null")
        self.filtered.registerTempTable("filtered_osm")

        #For the content only use the elements that are most recent, in this case have the highest timestamp
        self.osm_recent_data = sqlCtx.sql("select o.* from filtered_osm o, \
            (select id, max(timestamp) as max_time from filtered_osm group by id) time\
            where time.id = o.id and o.timestamp=time.max_time")

        self.osm_recent_data.registerTempTable("osm_recent_data")

        self.content_relations = content_relations

        #if no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}


    def get_user_vector(self):

        if self.user_vector_type=='num_edits':
            user_info = self.sqlCtx.sql("select uid, id, count(1) as rating from filtered_osm group by uid, id")\
                .map(lambda user_item_interact:(int(user_item_interact[0]), int(user_item_interact[1]), user_item_interact[2]))
            return user_info

        elif self.user_vector_type=='any_interact':
            user_info = self.sqlCtx.sql("select uid, id, 1 as rating from filtered_osm group by uid, id")\
                .map(lambda user_item_interact1:(int(user_item_interact1[0]), int(user_item_interact1[1]), user_item_interact1[2]))
            return user_info

        elif self.user_vector_type=='num_edits_ceil':
            user_info = self.sqlCtx.sql("select uid, id, count(1) as rating from filtered_osm group by uid, id") \
                                   .map(lambda user_item_interact2 : (int(user_item_interact2[0]), int(user_item_interact2[1]), min(user_item_interact2[2], 5)))
            return user_info

        elif self.user_vector_type=='none':
            return None

        else:
            print("Please choose a user_vector_type between 'num_edits', 'any_interact', 'num_edits_ceil', and 'none'")
            return None

    def get_content_vector(self):

        if self.content_vector_type=='tags_only':
            content_array = self.osm_recent_data.map(lambda row: (int(row.id), osm_vectorize_row(row, None)))
            fuller_content = self.inherit_properties(content_array)
            return fuller_content

        elif self.content_vector_type=='tags_w_geo':
            shp_path = self.support_files["shape_file"]
            sf = shapefile.Reader(shp_path)
            content_array = self.osm_recent_data.map(lambda row: (int(row.id), osm_vectorize_row(row, sf)))
            fuller_content = self.inherit_properties(content_array)
            return fuller_content

        elif self.content_vector_type=='none':
            return None

        else:
            print("Please choose a content_vector_type between 'tags_only', tags_w_geo or 'none'")
            return None


    def inherit_properties(self, orig_content_array):

        #have to have the ways inherrit their info first and then the relations
        #this is because relations consist of both ways and nodes

        content_with_types = self.osm_recent_data.map(lambda row: (int(row.id),str(row.osm_type))).join(orig_content_array)\
            .map(lambda n_id_o_type_cv: (n_id_o_type_cv[0], (n_id_o_type_cv[1][0], n_id_o_type_cv[1][1])))

        #have to have the ways inherrit their info first and then the relations
        #this is because relations consist of both ways and nodes

        way_relations = self.content_relations.filter(lambda row: row[2]=='way')

        inheret_info = way_relations.map(lambda w_n_w_t_n_t: (w_n_w_t_n_t[1],w_n_w_t_n_t[0])).join(orig_content_array)\
            .map(lambda n_w_c:(n_w_c[1][0],[n_w_c[1][1]])).combineByKey(lambda first:first, \
                    lambda com_vals, new_val : com_vals + new_val,\
                    lambda com_val1, com_val2 : com_val1+com_val2)\
            .map(lambda way_id_vals: (way_id_vals[0], np.max(way_id_vals[1], axis=0)))

        way_content = content_with_types.filter(lambda n_id_o_type_cv3: n_id_o_type_cv3[1][0]=='Way')\
                .map(lambda n_id_o_type_cv4: (n_id_o_type_cv4[0], n_id_o_type_cv4[1][1]))

        full_way_info = inheret_info.rightOuterJoin(way_content).map(lambda way_id_w_in_w_orig: \
            (way_id_w_in_w_orig[0], combine_cv(way_id_w_in_w_orig[1][0], way_id_w_in_w_orig[1][1])))

        #now get the relation information

        relation_relations = self.content_relations.filter(lambda row: row[2]=='relation')

        relation_info = relation_relations.map(lambda w_n_w_t_n_t5: (w_n_w_t_n_t5[1],w_n_w_t_n_t5[0])).join(orig_content_array)\
            .map(lambda n_w_c6:(n_w_c6[1][0],[n_w_c6[1][1]])).combineByKey(lambda first:first, \
                    lambda com_vals, new_val : com_vals + new_val,\
                    lambda com_val1, com_val2 : com_val1+com_val2)\
            .map(lambda way_id_vals7: (way_id_vals7[0], np.max(way_id_vals7[1], axis=0)))

        relation_content = content_with_types.filter(lambda n_id_o_type_cv8: n_id_o_type_cv8[1][0]=='Relation')\
            .map(lambda n_id_o_type_cv9: (n_id_o_type_cv9[0], n_id_o_type_cv9[1][1]))

        full_relation_info = relation_info.rightOuterJoin(relation_content).map(lambda way_id_w_in_w_orig10: \
                (way_id_w_in_w_orig10[0], combine_cv(way_id_w_in_w_orig10[1][0], way_id_w_in_w_orig10[1][1])))

        #now that we have ways and relations we just add the node content info
        nodes_vectors = content_with_types.filter(lambda n_id_o_type_cv11: n_id_o_type_cv11[1][0]=='Node')\
                .map(lambda n_id_o_type_cv12: (n_id_o_type_cv12[0], n_id_o_type_cv12[1][1]))

        #and put all three of them together again
        #is there a better way to do this - probably...does this work though - yes :)
        full_info = nodes_vectors.union(full_way_info).union(full_relation_info)

        return full_info

def combine_cv(w_in, w_orig):
    #This is used in a leftOuterJoin, so when the first
    try:
        return np.max((list(w_in),list(w_orig)), axis=0)
    except:
        return w_orig


def osm_vectorize_row(row, sf):
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

    if sf:
        if row.lon:
            lon = float(row.lon)
        else:
            lon = None
        if row.lat:
            lat = float(row.lat)
        else:
            lat = None
        vect.extend(fill_geo_content(sf, lon, lat))

    return vect


def fill_geo_content(shpPoly, x, y):
    poly_array = np.zeros(len(shpPoly.shapeRecords()), dtype=np.int)
    if x is not None and y is not None:
        div_idx = getDivsionName(shpPoly, x, y)
        if div_idx == -1:
            return poly_array
        else:
            poly_array[div_idx] = 1
            return poly_array
    else:
        return poly_array

def getDivsionName(shpPoly, x, y):
    idx = 0
    for poly in shpPoly.shapeRecords():
        if point_in_poly(x,y, poly.shape.points):
            #print poly.record[4] #the [4] item is the name in this shapefile
            return idx
        else:
            pass
    idx += 1
    return -1

def point_in_poly(x,y,poly):
    """" Ray Casting Method:
    Drawing a line from the point in question and stop drawing it when the line
    leaves the polygon bounding box. Along the way you count the number of times
    you crossed the polygon's boundary.  If the count is an odd number the point
    must be inside.  If it's an even number the point is outside the polygon.
    So in summary, odd=in, even=out
    """

    n = len(poly)
    inside = False

    p1x,p1y = poly[0]
    for i in range(n+1):
        p2x,p2y = poly[i % n]
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xints = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xints:
                        inside = not inside
        p1x,p1y = p2x,p2y

    return inside