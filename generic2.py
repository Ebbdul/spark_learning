import xml.etree.ElementTree as ET
import pandas as pd

tag1 = set()
header2 = []
tag2 = set()
tag3 = set()
list1 = []
tree = ET.parse('20xml.xml')
root = tree.getroot()
# print(root[0])


""" In this section we only work on getting tag of document"""

for child in root.find('*'):
    # print(child.tag, child.text)
    if child.text != '\n\t\t\t':  # if the text is blank mean there is subchild it will ignore that and only add siblings into set tag1
        tag1.add(child.tag)
    else:  # in else it will add the tags having children into set header2
        header2.append(child.tag)
# print(header2)
# header2 = list(header2)  # No need to convert into list of ots already in list, converting set header2 into list header2

child1_tag = header2[0]  # accessing the element present at 0 index
# print(child1_tag)
# child1_tag = './/' + child1_tag  # You can also pass it in find tag, no need to concate, creating a string that will use in find parameter
# print(child1_tag)
child2_tag = header2[1]  # You can also pass it in find tag, no need to concate, accessing the element present at 1 index
# print(child2_tag)
# child2_tag = './/' + child2_tag  # creating a string that will use in find parameter
# print(child2_tag)
for schild in root.find('.//' + child1_tag):  # passsing that parameter to find
    # print(schild.tag)
    tag2.add(schild.tag)  # getting child tag of that desired find's parameter and appending to set tag2
for schild2 in root.find('.//' + child2_tag):  # passsing that parameter to find
    # print(schild2.tag)
    tag3.add(schild2.tag)  # getting child tag of that desired find's parameter and appending to set tag2

tag1 = list(tag1)  # converting set tag1 to list
tag2 = list(tag2)  # converting  set tag2 to list
tag3 = list(tag3)  # converting  set tag3 to list
# print(tag3)

tag1 = sorted(tag1)  # parent tag sorting
tag2 = sorted(tag2)  # child tag1 sorting
tag3 = sorted(tag3)  # child tag3 sorting
# print(tag2)
""" Now we will work with text of these tag"""
# print(tag1)     #parent tag
# print(tag2)     #child tag 1
# print(tag3)     #child tag 2
dict1 = {}
dict2 = {}
dict3 = {}


def parent_text(tags):
    max_length = 0
    for tag in tags:
        dict1[tag] = []  # Initialize empty list for each tag
        parents = root.findall('*' + tag)
        if len(parents) > max_length:
            max_length = len(parents)

        for parent in parents:
            if parent.text != '\n\t\t\t':
                parent_text = parent.text.strip() if parent.text else None
                dict1[tag].append(parent_text)
                # Pad lists with None to have equal lengths
    for tag in tags:
        if len(dict1[tag]) < max_length:
            dict1[tag] += [None] * (max_length - len(dict1[tag]))


def child1_text(tags2):
    max_length = 0
    """  For child 1  """
    for t in tags2:
        dict2[t] = []
        childs1 = root.findall('.//' + t)
        if len(childs1) > max_length:
            max_length = len(childs1)
        for ch1 in childs1:
            if ch1.text != '\n\t\t\t':
                # print(ch1.tag)
                dict2[t].append(ch1.text)
            else:
                # dict2[t].append(None)
                print('none')

    # Pad lists with None to have equal lengths
    for t in tags2:
        if len(dict2[t]) < max_length:
            dict2[t] += [None] * (max_length - len(dict2[t]))
def child2_text(tags3):
    max_length = 0
    """  For child 2"""
    for t2 in tags3:
        dict3[t2] = []
        childs2 = root.findall('.//' + t2)
        if len(childs2) > max_length:
            max_length = len(childs2)

        for ch2 in childs2:
            child2_text = ch2.text.strip() if ch2.text else None
            dict3[t2].append(child2_text)

    # Pad lists with None to have equal lengths
    for t2 in tags3:
        if len(dict3[t2]) < max_length:
            dict3[t2] += [None] * (max_length - len(dict3[t2]))




parent_text(tag1)
child1_text(tag2)
child2_text(tag3)
# df1 = pd.DataFrame(dict1)
# print(df1)
# df1.to_csv('df1.csv')
df2 = pd.DataFrame(dict2)
print(df2)
# df2.to_csv('dict2.csv')
# df3 = pd.DataFrame(dict3)
# print(df3)
# df3.to_csv('dict3.csv')