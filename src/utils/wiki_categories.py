import rdflib
from rdflib.graph import Graph
import pandas as pd



graph_list = {}

def create_linked_list(file_path):
    """
    Transform the category links as formatted in the dbPedia Categories (Skos) into [parent, child] edges
    File may be downloaded from http://wiki.dbpedia.org/Downloads2015-04

    Args:
        file_path: link to where the original file is located
        top_name: the top name of the category graph: typically Fundamental Categories

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

    category_list = pd.DataFrame(category_list, columns=['parent','child'])

    return category_list

def linked_list_to_dict(graph_list, formated_df, top_name = 'Category:Fundamental_categories'):

    print 'Creating graph dictionary from linked elements'
    #start by seeding the top category
    graph_list = {}
    fundamentals = formated_df.query('parent=="%s"'%top_name)
    elem_list = []
    for (idx,row) in fundamentals.iterrows():
        elem_list.append(row['child'])
    graph_list[top_name] = elem_list
    #now from this top category get all other elements
    populate_elem(graph_list, top_name, elem_list,formated_df)

    return graph_list


def populate_elem(graph_list, elem_name, elem_list, formated_df):
    for elem in elem_list:
        if elem not in graph_list.iterkeys():
            print elem
            elem_list = get_children_elements(elem, formated_df)
            #by appending the parent name we are ensuring that the data is bi-directional
            elem_list.append(elem_name)
            graph_list[elem] = elem_list

            #however when we iterate we definitely do not want to get the parent again
            #adding it to the graph list above though helps to remove any circular references
            #elem_list.remove(elem_name)
            populate_elem(graph_list, elem, elem_list, formated_df)

            if len(graph_list)%1000==0:
                print len(graph_list), "graph elements created"


def get_children_elements(parent_name, formated_df):
    string_name = 'parent=="%s"'%parent_name
    children_list = formated_df.query(string_name)
    elem_list = []
    for (idx,row) in children_list.iterrows():
        elem_list.append(row['child'])
    return elem_list


def find_shortest_path(graph, start, end, path=[]):
    #Code snippit from https://www.python.org/doc/essays/graphs/
    #Likely not the fastest way to implement this, but for now works as long as graph is bi-directional
        path = path + [start]
        if start == end:
            return path
        if not graph.has_key(start):
            return None
        shortest = None
        for node in graph[start]:
            if node not in path:
                newpath = find_shortest_path(graph, node, end, path)
                if newpath:
                    if not shortest or len(newpath) < len(shortest):
                        shortest = newpath
        return shortest
