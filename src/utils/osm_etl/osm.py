#!/usr/bin/env python

"""Translate the OSM dataset to JSON.

This script takes an OSM file and turns it into a JSON object.
The OSM  files contain nodes (point data), ways (lines connecting points), and relations
Some OSM files also contain Changesets which associate all objects a user edited in one session.
The Changeset information could be viewed graph information, where objects that are edited together are related to each other.
However, this same information can be pulled from the object information and thus is not necessary.

In total there are over 57,000+ tags which users attribute to objects.
We are currently only concerned with some of these.
At the moment, all information is stored in the dictionary as strings for simplicity of coding.

Attributes:
    CHANGESET (dict): A dictionary of a user's interactions with the OSM database:
            - id: A unique identifier for each changeset.
            - created_at: Time of edit start
            - closted_at: Time of edit end
            - open (Boolean): Edit currently ongoing
            - user: User name
            - uid: Unique user ID
            - min/max lat/lon: The min and max lat and lon
            - num_changes: Number of changes made during the session
            - comments_count: Number of comments made during the session
     OSM_OBJECT (dict): A dictionary that stores OSM data, including revisions. The variables are as follows:
            - id: A unique identifier for each node, way or relation.
            - timestamp: Time of creation/revision
            - version: Version of the object
            - changeset: Unique identifier for the changeset
            - visible (Boolean): Visible on the map or hidden
            - user: Name of creator/revisionist of object for particular version
            - uid: Unique user ID of creator/revisionist of object for particular version
            - type: Choice between: Node, Way, and Relation
            - lat: Latitutude (Node only)
            - lon: Longitude (Node only)
            - user tags...

"""

import json
from copy import deepcopy
import xml.etree.cElementTree as ET



#JSON changeset object:
CHANGESET = {
    "id": None,
    "created_at": None,
    "closed_at": None,
    "open":None,
    "user":None,
    "uid":None,
    "min_lat":None,
    "min_lon":None,
    "max_lat":None,
    "max_lon":None,
    "num_changes":None,
    "comments_count":None
    }

#JSON osm objects (node, way, or relation):
OSM_OBJECT = {
    "id": None,
    "lat": None,
    "lon": None,
    "timestamp": None,
    "version": None,
    "changeset": None,
    "visible": None,
    "user": None,
    "uid": None,
    "osm_type": None, #can be Node, Way or Relation
    #add in tags
    "source": None,
    "building": None,
    "highway": None,
    "name": None,
    "addr_city": None,
    "addr_postcode": None,
    "natural": None,
    "landuse": None,
    "surface": None,
    "waterway": None,
    "power": None,
    "wall": None,
    "oneway": None,
    "amenity": None,
    "ref": None,
    "building_levels": None,
    "maxspeed": None,
    "barrier": None,
    "type": None,
    "place": None,
    "foot": None,
    "bicycle": None,
    "railway": None,
    "leisure": None,
    "bridge": None,
    "parking": None,
    "man_made": None,
    "railway": None,
    "aeroway": None,
    "wikipedia": None,
    "route": None
    }

if __name__ == "__main__":

    import argparse

    # Set up command line flag handling
    parser = argparse.ArgumentParser(
            description="Transform the OSM data to JSON",
        )
    parser.add_argument(
            'osm_history',
            type=str,
            help="the file containing the OSM data including changesets, something like 'osm_history.osm'",
            )
    parser.add_argument(
            '-o',
            '--output-directory',
            type=str,
            action="store",
            help="the directory to save the output OSM JSON files, by default the current directory",
            default="./",
            )
    args = parser.parse_args()

    tag_names2 = ['source', 'building', 'highway', 'name', 'addr_city', 'addr_postcode', 'natural', 'landuse', 'surface',\
            'waterway','power','wall','oneway','amenity','ref', 'building_levels', 'maxspeed','barrier','type','place',\
            'foot','bicycle','railway','leisure','bridge', 'parking','man_made','railway','aeroway', 'wikipedia']

    changeset_json_file = open( args.output_directory +"/changeset_test.json", 'w')
    node_json_file = open( args.output_directory +"/node_test.json", 'w')
    relation_map = open( args.output_directory +"/relation_map.txt", 'w')

    osm_tree = ET.iterparse(args.osm_history, events=("start", "end"))

for event, elem in osm_tree:
    if event == 'start' and elem.tag=='changeset':
        #print elem, elem.attrib
        #create a new changeset object
        c = deepcopy(CHANGESET)
        for key, value in elem.attrib.iteritems():
            c[key] = value #for now all values are strings
        #print json.dumps(c)
        changeset_json_file.write(json.dumps(c) + "\n")

    elif event == 'start' and elem.tag=='node':
        #create a new node object
        n = deepcopy(OSM_OBJECT)
        for key, value in elem.attrib.iteritems():
            #print key, value
            key = key.replace(":", "_")
            n[key] = value #for now all values are strings
        n["osm_type"] = 'Node'
        for child in elem:
            if child.tag =='tag':
                k, v = child.attrib.items()
                key = k[1]
                value = v[1]
                key = key.replace(":", "_")
                if key in tag_names2:
                    n[key] = value
                #print key, value

    elif event=='end' and elem.tag=='node':
        #print json.dumps(n)
        node_json_file.write(json.dumps(n) + "\n")

    elif event == 'start' and elem.tag=='way':
        #create a new node object of type way
        n = deepcopy(OSM_OBJECT)
        nid = elem.get('id')
        for key, value in elem.attrib.iteritems():
            n[key] = value #for now all values are strings
        n["osm_type"] = 'Way'
        for child in elem:
            if child.tag =='tag':
                k, v = child.attrib.items()
                key = k[1]
                value = v[1]
                key = key.replace(":", "_")
                if key in tag_names2:
                    n[key] = value
            elif child.tag =='nd':
                ref_id = child.get('ref')
                #these are the node references, we will create a file that is w_id, n_id, w_type, n_type
                relation_map.write(nid + ',' + ref_id + ', way, node' + "\n")
            else:
                print child.tag

    elif event=='end' and elem.tag=='way':
        #print json.dumps(n)
        node_json_file.write(json.dumps(n) + "\n")


    elif event == 'start' and elem.tag=='relation':
        #create a new node object of type way
        n = deepcopy(OSM_OBJECT)
        nid = elem.get('id')
        for key, value in elem.attrib.iteritems():
            key.replace(":", "_")
            n[key] = value #for now all values are strings
        n["osm_type"] = 'Relation'
        for child in elem:
            if child.tag =='tag':
                k, v = child.attrib.items()
                key = k[1]
                value = v[1]
                key = key.replace(":", "_")
                if key in tag_names2:
                    n[key] = value
            elif child.tag =='member':
                ref_id = child.get('ref')
                r_type = child.get('type')
                #these are the node references, we will create a file that is w_id, n_id, w_type, n_type
                relation_map.write(nid + ',' + ref_id + ', relation, '+ r_type + "\n")
            else:
                print child.tag

    elif event=='end' and elem.tag=='way':
        #print json.dumps(n)
        node_json_file.write(json.dumps(n) + "\n")


    changeset_json_file.close()
    node_json_file.close()
    relation_map.close()