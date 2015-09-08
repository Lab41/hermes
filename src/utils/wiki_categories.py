import rdflib
from rdflib.graph import Graph


category_list = []

def get_linked_list(file_path):
    """
    Transform the category links as formatted in the dbPedia Categories (Skos) into [parent, child] edges
    File may be downloaded from http://wiki.dbpedia.org/Downloads2015-04

    Args:
        file_path: link to where the original file is located

    Retruns:
        category_list: List of parent_child edges
    """


    g = rdflib.Graph()
    result = g.parse(file_path, format="nt")
    category_list = []

    for stmt in g:
        if type(stmt[2])==rdflib.term.URIRef:
            page_name = str.split(str(stmt[0]), '/')[-1]
            parent_name = str.split(str(stmt[2]), '/')[-1]
            type_relationship = str.split(str(stmt[1]), '/')[-1]
            #filter out anything where parent_name is not a true category name (like core#Concept)
            #also filter out anything which is not of type core#broader
            #there is also
            if str.split(str(parent_name), ':')[0]=='Category' and str.split(str(page_name), ':')[0]=='Category' and \
                    type_relationship =='core#broader':
                category_list.append([parent_name, page_name])

    print ('Number of elements:', len(category_list))
    return category_list
