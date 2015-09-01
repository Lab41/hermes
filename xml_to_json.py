#!/usr/bin/env python

import xml.etree.cElementTree as ET
from sys import argv

input_file = argv[1]

NAMESPACE = "{http://www.mediawiki.org/xml/export-0.10/}"

with open(input_file) as open_file:
    in_page = False
    for _, elem in ET.iterparse(open_file):
        # Pull out each revision
        if elem.tag == NAMESPACE + "revision":
            # Look at each subtag, if it is the 'sha1' tag, print out the text content
            for child in elem:
                if child.tag == NAMESPACE + "sha1":
                    print child.text
                # Clear the child to free up memory
                child.clear()
            # Now clear the parent once we've finished with it to further clean up
            elem.clear()
