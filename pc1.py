import xml.etree.ElementTree as ET
import pandas as pd

tree = ET.parse('20xml.xml')
root = tree.getroot()

store_items = []
all_items = []
like_items = []
all_like_items = []
multi_items = []
all_multi_items = []

for child in root.findall('feeds'):
    if child.find('id').text is not None and child.find('title').text is not None and child.find('description').text is not None \
            and child.find('location').text is not None and child.find('lng').text is not None and child.find('lat').text is not None \
            and child.find('userId').text is not None and child.find('name').text is not None and child.find('isdeleted').text is not None \
            and child.find('profilePicture').text is not None and child.find('mediatype').text is not None and child.find('commentCount').text is not None \
            and child.find('createdAt').text is not None and child.find('code').text is not None:
        pid = child.find('id').text
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
        store_items = [pid, title, description, location, lng, lat, userId, name, isdeleted, profilePicture, mediatype,
                       commentCount, createdAt, code]
        all_items.append(store_items)

        for subchild in child.findall('likeDislike'):
            likes = subchild.find('likes').text
            dislikes = subchild.find('dislikes').text
            userAction = subchild.find('userAction').text
            like_items = [pid,likes, dislikes, userAction]
            all_like_items.append(like_items)

        for schild in child.findall('multiMedia'):
            id = schild.find('id').text
            name = schild.find('name').text
            description = schild.find('description').text
            url = schild.find('url').text
            mediatype = schild.find('mediatype').text
            likeCount = schild.find('likeCount').text
            createAt = schild.find('createAt').text
            multi_items = [pid, id, name, description, url, mediatype, likeCount, createAt]
            all_multi_items.append(multi_items)
print(all_items)
feed_columns = ['pid', 'title', 'description', 'location', 'lng', 'lat', 'userId', 'name', 'isdeleted', 'profilePicture',
                'mediatype', 'commentCount', 'createdAt', 'code']
feeds_df = pd.DataFrame(all_items, columns=feed_columns)
# feeds_df['index'] = feeds_df.reset_index().index
# feeds_df = feeds_df.rename(columns={'index': 'feeds_id'})

like_columns = ['pid','likes', 'dislikes', 'userAction']
likes_df = pd.DataFrame(all_like_items, columns=like_columns)
# likes_df['index'] = likes_df.reset_index().index
# likes_df = likes_df.rename(columns={'index': 'likes_id'})

multi_columns = ['pid','id', 'name', 'description', 'url', 'mediatype', 'likeCount', 'createAt']
multi_df = pd.DataFrame(all_multi_items, columns=multi_columns)
# multi_df['index'] = multi_df.reset_index().index
# multi_df = multi_df.rename(columns={'index': 'multi_id'})

merged_df=pd.merge(feeds_df, multi_df, on='pid')
# merged_df.to_csv('merged_df.csv')
# print(feeds_df)
# print(likes_df)
# print(multi_df)
# print(merged_df)