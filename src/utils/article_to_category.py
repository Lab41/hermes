import numpy as np
import networkx as nx
import re


class article_to_category():

    """
        Class to map wikipedia articles to a category vector which can then be used as a content vector for content based recommender systems.
        To run this class you need to have run some of the functions in wiki_categories.py to create the graph links and category index.

        Example of the class being run could be:

        -----------
        from pyspark.sql.types import StructType
        import json
        import pickle

        #load and create the files necessary
        category_idx = pickle.load( open("category_index.p", 'rb'))
        high_level_categories = ['Geography', 'Health', 'History', 'Humanities', 'Literature', 'Mathematics', \
               'Nature', 'People', 'Philosophy', 'Reference_works', 'Science', 'Technology']
        ac = article_to_category(high_level_categories, "category_index_graph_link", category_idx)

        #bring in the wikipedia edits, using the json schema if you have it as it is a lot faster!
        with open('wikipedia_full_text_schema.json', 'r') as json_schema_file:
            schema = StructType.fromJson(json.load(json_schema_file))
        wiki_rdd = sqlCtx.read.json('wiki_edits.json.gz', schema = schema)

        article_mapping = ac.run_mapping(wiki_rdd)
        -----------

    """

    def __init__(self, high_level_categories, category_index_graph_link, category_idx):
        """
        Class initializer to Loads the required files

        Args:
            high_level_idx: An array of the high level categories to map to e.g. ['Concepts', 'Life', 'Physical_universe', 'Society']
            category_index_graph_link: Path to the csv of the category links as created from wiki_categories.create_linked_list()
            category_idx: Dictionary of the categories to an index as created from wiki_categories.create_category_idx_dicts()

        """
        #If the format does not have 'Category' in front like our example above we must add that in
        main_topics = ['Category:' +a for a in high_level_categories]
        self.high_level_idx = [category_idx[a] for a in main_topics]

        #Load the Wikipedia category graph from the graph link.  This is assumed to be a csv file
        #File can be created using code from wiki_categories.create_linked_list()
        dg = nx.DiGraph()
        self.category_index_graph = nx.read_adjlist(category_index_graph_link, 'rb', \
                                       create_using=dg ,delimiter=',', nodetype = int)
        self.category_idx = category_idx


    def run_mapping(self, wiki_rdd):
        """
        Maps articles in a wikipedia dump to the high level categories.
        Will only return articles with a findable category mapping, so will skip those articles without categories
        or for whom the mapping could otherwise not be found.

        Args:
            high_level_idx: the full wikipedia text loaded into an RDD (likely through sqlCtx.read.json)

        Returns:
            article_map: mapping of articles to the top level categories
        """

        #Filter out uninteresting articles
        filtered_rdd = wiki_rdd.rdd.filter(
            lambda row:
            row.redirect_target is None  # Save only edits that are not redirects
            and row.article_namespace == 0  # Save only edits on articles, not talk pages
            and row.full_text is not None  # Has text
        )

        #run the category mapping filtering out any articles where the mapping was not found
        article_map = filtered_rdd.map(lambda row: (row.article_title, row.article_id, extract_categories(row.full_text))) \
            .map(lambda (article_title, article_id, cats): self.category_mapping(article_title, article_id, cats)) \
            .filter(lambda row: row != None)
        return article_map


    def category_mapping(self, article_title, article_id, cats):
        parent_lengths_all = []
        #only try to determine the category mapping if the article contains categories
        #there are only a few articles without any categories
        if len(cats)>0:
            for c in cats:
                c = clean_name(c)
                cat_idx = self.category_idx[c]
                #If the category cannot be determined from the category index
                parent_lengths = np.ones(len(self.high_level_idx))*1000000000
                if cat_idx!=[]:
                    parent_lengths = self.getParentArray(cat_idx)
                parent_lengths_all.append(parent_lengths)

            #a shorter length is better so the shortest path will receive a 1 and the longest a 0...the rest will scale in between
            #also if there is at least one array with values (not the 10000000 vector) then the 100000 vectors should be removed first
            #if there are no arrays with values then no array will be returned
            if np.mean(parent_lengths_all, axis=0)[0]<1000000000:
                clean_list = []
                for pl in parent_lengths_all:
                    if pl[0]!=1000000000 and pl[0]!=0.:
                        clean_list.append(pl)
                avg = np.mean(clean_list, axis=0)
                min_val = min(avg)
                max_val = max(avg)
                diff = max_val-min_val
                if diff==0: diff=1
                content_vector = []
                for t in avg:
                    new_val = ((t-min_val)**0*(max_val-t))/diff
                    content_vector.append(new_val)
                return [article_id, content_vector]


    def getParentArray(self, catID):
        """
        From a given category ID, the function will then find the shortest path to the high level index list
        If  a path from the given category to a top level category cannot be found, then the distance will be 1000000000

        Args:
            catID: the originating category ID

        Returns:
            parent_lengths: the distance in the graph between the category ID and top level categories
        """
        parent_lengths = []
        for main_idx in self.high_level_idx:
            dist = 1000000000
            try:
                paths = nx.shortest_path(self.category_index_graph, main_idx, catID)
                dist = len(paths)
            except:
                pass
            parent_lengths.append(dist)
        return parent_lengths

def clean_name(catName):
    """
    Cleans the category name so that it matches the same format as the category list
    #TODO expand this list of cleans or determine encoding difference

    Args:
        catName: original category name as given in the wikipedia dump

    Returns:
        cleanName: the cleaned category name which matches the dbPedia dump
    """
    cleanName = catName.replace(' ', '_')
    cleanName = cleanName.replace('\u2013', '-')
    return cleanName

def extract_categories(text):
    """Extract the Wikipedia categories from the full text of an article.

    Returns a list of strings enumerating the categories for a Wikipedia
    article by parsing the full text of the article. The strings are of the
    form "Category:Foo" and do not contain the opening or closing square
    brackets. If the article contains a sorting hint in the category (for
    example, "[[Category:Foo|Sorting Hint]]") those are removed before
    returning the category.

    This function uses a regular expression: \[\[(Category:[^\[\]|]*)

    This expression requires that the matched text starts with "[[" but does
    not look for a closing "]]" as these can be arbitrarily far away if a
    sorting hint is included.

    Args:
        text (str): The full text of a Wikipedia article in one string.

    Returns:
        list: A list containing strings of the categories a page belongs to.

        The list is empty if the page belongs to no categories.

    """
    # Since Regexes are unreadable, let me explain:
    #
    # "\[\[Category:[^\[\]]*)" consists of several parts:
    #
    #    \[\[ matches "[["
    #
    #    (...) is a capture group meaning roughly "the expression inside this
    #    group is a block that I want to extract"
    #
    #    Category: matches the text "Category:"
    #
    #    [^...] is a negated set which means "do not match any characters in
    #    this set".
    #
    #    \[, \], and | match "[", "]", and "|" in the text respectively
    #
    #    * means "match zero or more of the preceding regex defined items"
    #
    #  So putting it all together, the regex does this:
    #
    #    Finds "[[" followed by "Category:" and then matches any number
    #    (including zero) characters after that that are not the excluded
    #    characters "[", "]", or "|". When it hits an excluded character it
    #    stops and the text matched by the regex inside the (...) part is
    #    returned.
    return re.findall(r'\[\[(Category:[^\[\]|]*)', text)