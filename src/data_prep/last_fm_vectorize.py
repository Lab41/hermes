import numpy as np

class last_fm_vectorize():

    def __init__(self, user_interactions, content, user_vector_type, content_vector_type, sqlCtx, **support_files):
        """
        Class initializer to load the required files.

        Args:
            user_interactions: The raw RDD of the user interactions. For Last FM, this it the number of artist plays.
                We have been reading it in as plays = sqlCtx.read.json(last_fm_play_data_path, schema=schema)
            content: The raw RDD containing the item content. For Last FM, these are the applied tags
                The tag table can map to the tag applied, but we don't need this info
            user_vector_type: The type of user vector desired.  For Last FM you can choose between ['num_plays', 'any_interact', 'num_plays_log', 'none'].
                num_plays_log uses log base 10
                If 'none' is used then this means you will run your own custom mapping
            content_vector_type: The type of content vector desired. For Last FM you can choose between ['tags', 'none'].
                If none is chosen no content vector will be returned and None may be passed into the content argument.
                You do not need a content vector to run pure CF only but some performance metrics will not be able to be ran
            sqlCtx: The sequel content which is necessary for some of the queries
            support_files: If they exist, the supporting files, dataFrames, and/or file links necessary to run the content vectors.
                Here num_tags can be passed in which is the number of tags to consider in the content vector
        """
        self.user_vector_type = user_vector_type
        self.content_vector_type = content_vector_type
        self.sqlCtx = sqlCtx

        self.content = content

        #Filter out uninteresting articles and users if they still exist in the dataset
        user_interactions.registerTempTable("artist_plays")
        content.registerTempTable("tags")

        #if no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}


    def get_user_vector(self):

        if self.user_vector_type=='num_plays':
            user_info =  self.sqlCtx.sql("select user_id as user, artist_id as item, plays from artist_plays").rdd

            return user_info

        elif self.user_vector_type=='any_interact':
            user_info =  self.sqlCtx.sql("select user_id as user, artist_id as item, 1 as play from artist_plays").rdd

            return user_info

        elif self.user_vector_type=='num_plays_log':
            user_info =  self.sqlCtx.sql("select user_id as user, artist_id as item, plays from artist_plays")\
                .map(lambda user_article_plays: (user_article_plays[0], user_article_plays[1], np.log10(user_article_plays[2])))

            return user_info

        elif self.user_vector_type=='none':
            return None

        else:
            print("Please choose a user_vector_type between num_plays, any_interact, num_plays_log or none")
            return None


    def get_content_vector(self):
        if self.content_vector_type=='tags':

            try:
                num_tags = self.support_files['num_tags']
            except:
                num_tags = 150

            #get the top 150 tags to apply as a content vector
            tag_count = self.sqlCtx.sql("select tag_id, count(1) as tag_count from tags group by tag_id").collect()
            top_tags = sorted(tag_count, key=lambda x: x[1], reverse=True)[:num_tags]
            #get the tags into a happy list
            ts = []
            for elem in top_tags:
                ts.append(elem[0])

            #get the tags by artist
            #we will filter out any artists without any tags - there are about 1,500 of these for num_tags=150
            tag_vector = self.content.map(lambda row: (row.artist_id, row.tag_id)).groupByKey().map(lambda row: get_vect(row, ts))\
                    .filter(lambda artist_id_cv: sum(artist_id_cv[1])!=0)

            return tag_vector


        elif self.content_vector_type=='none':
            return None

        else:
            print("Please choose between tags or none")
            return None

def get_vect(row, keeper_tags):
    tag_vect = np.zeros(len(keeper_tags))
    for tag in row[1]:
        try:
            index = keeper_tags.index(tag)
            tag_vect[index] = 1
        except:
            pass
    return (row[0], tag_vect)