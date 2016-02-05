import numpy as np


class book_vectorize():

    def __init__(self, user_interactions, content, user_vector_type, content_vector_type, sqlCtx, **support_files ):
        """
        Class initializer to load the required files.

        Args:
            user_interactions: The raw RDD of the user interactions. For the Book-Crossing, these are the ratings or interactions
            content: The raw RDD containing the item content. For Book-Crossing, this is the book information
                This only includes the author, title, year and publisher so not a lot, but it is something
            user_vector_type: The type of user vector desired.  For Book-Crossing you can choose between ['positive_interact', 'interacts', 'ratings', 'none'].
                If 'none' is used then this means you will run your own custom mapping
            content_vector_type: The type of content vector desired. For Book-Crossing you can choose between ['content', 'none'].
                If none is chosen no content vector will be returned and None may be passed into the content argument.
            sqlCtx: The sequel content which is necessary for some of the queries

        """
        self.user_vector_type = user_vector_type
        self.sqlCtx = sqlCtx

        #Filter out uninteresting items and users if they still exist in the dataset
        self.user_interactions =user_interactions
        self.user_interactions.registerTempTable("book_ratings")

        #if no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}


    def get_user_vector(self):

        if self.user_vector_type=='positive_interact':
            user_info = self.sqlCtx.sql("select user_id, book_id, 1 as interact from book_ratings \
                        where (implicit=True or (rating is not NULL and rating>5))")\
                        .map(lambda (user, book, interact): (user, book_to_int(book), interact))\
                        .filter(lambda (user, book, interact): book!=-1)
            return user_info

        elif self.user_vector_type=='interacts':
            user_info = self.sqlCtx.sql("select user_id, book_id, 1 as interact from book_ratings \
                        where implicit=True")\
                        .map(lambda (user, book, interact): (user, book_to_int(book), interact))\
                        .filter(lambda (user, book, interact): book!=-1)
            return user_info

        elif self.user_vector_type=='ratings':
            user_info = self.sqlCtx.sql("select user_id, book_id, rating from book_ratings \
                        where rating is not NULL")\
                        .map(lambda (user, book, interact): (user, book_to_int(book), interact))\
                        .filter(lambda (user, book, interact): book!=-1)
            return user_info

        elif self.user_vector_type=='none':
            return None

        else:
            print "Please choose a user_vector_type between 'positive_interact', 'interacts', 'ratings', and 'none'"
            return None

    def get_content_vector(self):

        if self.content_vector_type=='content':
            #the the content features for each book
            content_array = content_features(self.user_interactions)
            return content_array

        elif self.content_vector_type=='none':
            return None

        else:
            print "Please choose a content_vector_type between 'content' or 'none'"
            return None

def content_features(sqlCtx, data_rdd):
    #set up the data by first grabbing all the potential features

    #get the list of authors with more than five reviews
    authors = sqlCtx.sql("select author, count(1) as cnt from books group by author")\
        .rdd.filter(lambda (author, cnt): cnt>5).map(lambda (author, c): author).distinct().collect()

    #get the list of publishers with more than five reviews
    publishers = sqlCtx.sql("select publisher, count(1) as cnt from books group by publisher")\
        .rdd.filter(lambda (publisher, cnt): cnt>5).map(lambda (publisher, c): publisher).distinct().collect()

    #get the list of years with more than five reviews
    years = sqlCtx.sql("select year, count(1) as cnt from books group by year")\
        .rdd.filter(lambda (year, cnt): cnt>5).map(lambda (year, c): year).distinct().collect()

    #features will go countries, page_field, pagename filled out by get_vect
    #we are filtering out any content where the book cannot be made into an int, or the content vector is null (very rare)
    content = data_rdd.map(lambda row: (book_to_int(row.book_id), get_vect(row, authors, publishers, years)))\
        .filter(lambda (b_id, vect): book_to_int(b_id)!=-1 or sum(list(vect))>0)

    return content

def get_vect(row, authors, publishers, years):

    author_vect = np.zeros(len(authors))
    try:
        index = authors.index(row.author)
        author_vect[index] = 1
    except:
        pass

    pub_vect = np.zeros(len(publishers))
    try:
        index = publishers.index(row.publisher)
        pub_vect[index] = 1
    except:
        pass

    year_vect = np.zeros(len(years))
    try:
        index = years.index(row.year)
        year_vect[index] = 1
    except:
        pass

    final_vect = np.concatenate((author_vect,pub_vect,year_vect), axis=0)
    return final_vect

def book_to_int(b_id):
    int_id = -1
    try:
        int_id = int(b_id)
    except:
        try:
            #a lot of times there is just an X on the back so lets keep those
            b_id = b_id[:-1]
            int_id = int(b_id)
        except:
            pass

    return int_id