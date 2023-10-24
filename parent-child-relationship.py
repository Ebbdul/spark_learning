import xml.etree.ElementTree as ET
import pandas as pd
tree = ET.parse('20xml.xml')
root = tree.getroot()
store_item=[]
all_item=[]
for child in root.findall('feeds'):
    id = child.find('id').text
    title = child.find('title').text
    description = child.find('description').text
    location = child.find('location').text
    lng = child.find('lng').text
    lat = child.find('lat').text
    userId = child.find('userId').text
    name = child.find('name').text
    isdeleted = child.find('isdeleted').text
    profilePicture = child.find('profilePicture').text
    mediatype = child.find('mediatype').text
    commentCount = child.find('commentCount').text
    createdAt = child.find('createdAt').text
    code = child.find('code').text
    likes=child.find('likeDislike').find('likes').text
    dislike=child.find('likeDislike').find('dislikes').text
    userAction=child.find('likeDislike').find('userAction').text
    multi_id=child.find('multiMedia').find('id').text
    multi_name=child.find('multiMedia').find('name').text
    multi_desc=child.find('multiMedia').find('description').text
    multi_url=child.find('multiMedia').find('url').text
    multi_mediatype=child.find('multiMedia').find('mediatype').text
    multi_likeCount=child.find('multiMedia').find('likeCount').text
    multi_createAt=child.find('multiMedia').find('createAt').text
    store_items = [id, title, description, location, lng, lat, userId, name, isdeleted, profilePicture, mediatype,
                   commentCount, createdAt, code, likes, dislike, userAction, multi_id, multi_name, multi_desc, multi_url,
                   multi_mediatype, multi_mediatype, multi_likeCount, multi_createAt]
    all_item.append(store_items)
col_name=[ 'id','title', 'description', 'location', 'lng', 'lat', 'userId', 'name', 'isdeleted', 'profilePicture', 'mediatype', 'commentCount', 'createdAt','code', 'likes', 'dislike', 'userAction', 'multi_id', 'multi_name', 'multi_desc', 'multi_url',
                   'multi_mediatype', 'multi_mediatype', 'multi_likeCount', 'multi_createAt']
df=pd.DataFrame(all_item, columns=col_name)
print(df)