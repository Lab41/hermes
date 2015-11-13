from math import log
import numpy as np

def tf_idf(content_vector):
    """
    Calculates the term frequency-inverse document frequency of each item.

    Example from www.tfidf.com

    Consider a document containing 100 words wherein the word cat appears 3 times. The term frequency (i.e., tf) for cat is then (3 / 100) = 0.03.
    Now, assume we have 10 million documents and the word cat appears in one thousand of these.
    Then, the inverse document frequency (i.e., idf) is calculated as log(10,000,000 / 1,000) = 4.
    Thus, the Tf-idf weight is the product of these quantities: 0.03 * 4 = 0.12.

    Args:
        content_vector: The full content space where each item is in the form  (item [content_features])

    Returns:
        content_vector_tf_idf: An RDD of (item, [content_features]) where now the content features are normalized by their tf-idf
    """
    corpus_idf = idf(content_vector)
    content_vector_tf_idf = content_vector.map(lambda (movie_id, array): (movie_id, tf_content(array, corpus_idf)))
    return content_vector_tf_idf


def idf(content_vector):
    """
    Calculates the inverse document frequency for the content space.  The content vector should be in the form (item [content_features])
    It is important to run this before the tf_content as the idf will be utilized when passed in.

    Args:
        content_vector: The full content space where each item is in the form  (item [content_features])

    Returns:
        item_idf: The inverse document frequency for each feature.
    """

    num_items = content_vector.count()
    items_local = content_vector.map(lambda (item_id, array): array).collect()
    col_totals = [ sum(x) for x in zip(*items_local) ]
    item_idf = []
    for c in col_totals:
        item_idf.append(log(num_items/c))
    return item_idf


def tf_content(array, idf=[]):
    """
    Calculates the term frequency for a specific item.  The content vector should be in the form (item [content_features])
    If idf is passed in, will return the tf-idf, but if not passed in only the tf will be returned.

    Args:
        array: The full content space where each item is in the form  (item [content_features])
        idf: the inverse document frequency of each attribute in the item corpus.  Determined by running idf(content_vector)

    Returns:
        tf_array: The vector reflecting the tf, or tf-idf for an item
    """

    if idf==[]:
        idf = np.ones(len(array))
    total_count = float(sum(array))
    #print total_count
    if total_count>0:
        tf_array = np.multiply(array/total_count, idf)
        return tf_array
    else:
        return array