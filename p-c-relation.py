import pandas as pd
import xml.etree.ElementTree as ET
from tabulate import tabulate
# Path to the XML file
xml_file_path = '20xml.xml'

# Parse the XML file
tree = ET.parse(xml_file_path)

# Get the root element of the XML file
root = tree.getroot()
# print(root.tag)
# Create empty lists to store the data
parent_data = []
child1_data = []
child2_data = []

# Process the XML data and extract the values
for feeds in root:
    for parent_index, parent_element in enumerate(feeds):
        if parent_element.tag != 'multiMedia' and parent_element.tag != 'likeDislike':
            parent_id = parent_index + 1
            # print(parent_element.tag, parent_element.text)
            parent_values = [parent_id, parent_element.tag, parent_element.text]
            # print(parent_values)
            parent_data.append(parent_values)
            # for child_element in feeds:
            #     # print(child_element.tag,child_element.text)
            #     if child_element.tag == 'likeDislike':
            #         # print(child_element.tag, child_element.text)
            #         for likedislike in child_element:
            #             # print(likedislike.tag, likedislike.text)
            #             child1_data.append(likedislike.text)
            #
            #     elif child_element.tag == 'multiMedia':
            #         for multimedia in child_element:
            #             # print(multimedia.tag, multimedia.text)
            #             child2_data.append(multimedia.text)

print(parent_data)