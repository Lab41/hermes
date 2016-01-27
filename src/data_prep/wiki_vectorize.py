from src.utils import article_to_category, glove, remove_templates, clean_categories, clean_links
import string
import numpy as np

class wiki_vectorize():

    def __init__(self, user_interactions, content, user_vector_type, content_vector_type, sqlCtx, **support_files):
        """
        Class initializer to load the required files

        Args:
            user_interactions: The raw RDD of the user interactions. For Wikipedia, this it is the full edit history.
                We have been reading it in as wiki_edits = sqlCtx.read.json(wiki_edit_json_data_path, schema=schema)
            content: The raw RDD containing the item content. For Wikipedia, this is the latest edit which contains full article content
            user_vector_type: The type of user vector desired.  For Wikipedia you can choose between ['num_edits', 'any_interact', 'num_edits_ceil', 'none'].
                num_edits_ceil will count the number of edits but set an upper limit of 5 edits
                If 'none' is used then this means you will run your own custom mapping
            content_vector_type: The type of content vector desired. For Wikipedia you can choose between ['glove', 'category_map', 'none'].
                If none is chosen no content vector will be returned and None may be passed into the content argument.
                You do not need a content vector to run pure CF only but some performance metrics will not be able to be ran
            support_files: If they exist, the supporting files, dataFrames, and/or file links necessary to run the content vectors.
                For example the category_map function at least needs the category_list from dbPedia

        """
        self.user_vector_type = user_vector_type
        self.content_vector_type = content_vector_type
        self.sqlCtx = sqlCtx

        #Filter out uninteresting articles and users if they still exist in the dataset
        user_interactions.registerTempTable("ratings")
        content.registerTempTable("content")

        filtered =  self.sqlCtx.sql("select * from ratings where redirect_target is null and article_namespace=0 and user_id is not null")
        filtered_content =  self.sqlCtx.sql("select * from content where redirect_target is null and article_namespace=0 and full_text is not null")

        self.filtered = filtered
        self.filtered.registerTempTable("wiki_ratings")

        self.filtered_content = filtered_content
        self.filtered_content.registerTempTable("wiki_content")

        #if no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}


    def get_user_vector(self):

        if self.user_vector_type=='num_edits':
            user_info =  self.sqlCtx.sql("select user_id as user, article_id as item, count(1) as rating from wiki_ratings \
                group by user_id, article_id")

            return user_info

        elif self.user_vector_type=='any_interact':
            user_info =  self.sqlCtx.sql("select user_id as user, article_id as item, 1 as rating from wiki_ratings \
                group by user_id, article_id")

            return user_info

        elif self.user_vector_type=='num_edits_ceil':
            user_info =  self.sqlCtx.sql("select user_id as user, article_id as item, count(1) as rating from wiki \
                group by user_id, article_id")\
                .map(lambda (user, article, rating): (user, article, min(rating, 5)))

            return user_info

        elif self.user_vector_type=='none':
            return None

        else:
            print "Please choose a user_vector_type between num_edits, any_interact, num_edits_ceil or none"
            return None


    def get_content_vector(self):
        if self.content_vector_type=='glove':

            if self.support_files==1:
                glove_model = self.support_files["glove_model"]

                article_mapping = self.filtered_content\
                        .map(lambda row: (row.article_id, remove_templates(row.full_text)))\
                        .map(lambda tup: (tup[0],clean_categories(tup[1])))\
                        .map(lambda tup: (tup[0],clean_links(tup[1])))\
                        .map(
                            lambda tup:
                            (tup[0], tup[1]\
                            .replace('\n', ' ')\
                            .replace("<ref>", '')\
                            .replace("</ref>", '')\
                            )
                        )\
                        .map(lambda tup: (tup[0], remove_punctuation(tup[1])))\
                        .map(lambda tup: (tup[0], remove_urls(tup[1])))\
                        .map(lambda tup: (tup[0], article_to_glove(tup[1], glove_model)))

                return article_mapping

            else:
                print "Please pass in a glove_model. Like: support_files['glove_model']=Glove('glove.6B.50d.txt')"
        elif self.content_vector_type=='category_map':

            if len(self.support_files)==3:
                    #The category map supporting dataFrames and objects are as followed:
                    #high_level_idx: An array of the high level categories to map to e.g. ['Concepts', 'Life', 'Physical_universe', 'Society']
                    #category_index_graph_link: Path to the csv of the category links as created from wiki_categories.create_linked_list()
                    #category_idx: Dictionary of the categories to an index as created from wiki_categories.create_category_idx_dicts()

                high_level_categories = self.support_files['high_level_categories']
                category_index_graph_link = self.support_files['category_index_graph_link']
                category_idx = self.support_file['category_idx']

                ac = article_to_category(high_level_categories, category_index_graph_link, category_idx)
                article_mapping = ac.run_mapping(self.filtered_content)

                return article_mapping

            else:
                #print "To run category map you must at least have the category_list from dbPedia"
                ##TODO work on the article_to_category function so that it can just pull in the category list from dpPedia
                print "Please pass in the following files:"
                print "high_level_idx: An array of the high level categories to map to e.g. ['Concepts', 'Life', 'Physical_universe', 'Society']"
                print 'category_index_graph_link: Path to the csv of the category links as created from wiki_categories.create_linked_list()'
                print 'category_idx: Dictionary of the categories to an index as created from wiki_categories.create_category_idx_dicts()'
                print 'support_files = {"high_level_categories" : high_level_categories, \
                 "category_index_graph_link" : category_index_graph_link, \
                 "category_idx" : category_idx}'
                return None

        elif self.content_vector_type=='none':
            return None

        else:
            print "Please choose between glove, category_map or none"
            return None

def remove_punctuation(text):
    for char in string.punctuation:
        text = text.replace(char, '')
    return text

def article_to_glove(text, model):
    vec = np.zeros(model.vector_size)
    for word in text.split():
        vec += model[word.lower()]

    return vec

def remove_urls(text):
    stext = text.split()
    next_text = []
    for word in stext:
        if word.startswith('http'):
            continue
        else:
            next_text.append(word)

    return ' '.join(next_text)