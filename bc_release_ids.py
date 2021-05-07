import pymongo
from tqdm import tqdm
import time
import os
from bs4 import BeautifulSoup
import requests
import json

mongodb_id = os.environ['MONGODB_ID']
mongodb_pw = os.environ['MONGODB_PW']

client = pymongo.MongoClient(f'mongodb+srv://{mongodb_id}:{mongodb_pw}@bc01-muwwi.gcp.mongodb.net/test?retryWrites=true&w=majority')
db = client.BC02
coll = db.artistInfo
artists = coll.find({'location':{'$exists':True},'latitude':{'$exists':True}, 'latest_release':{'$exists':False}})

# def spotify_update(artist):
#     q = artist['artist_name'].lower()
#     results = sp.search(q,type='artist')
#     if not results['artists']['items']:
#         return 0
#     else:
#         for item in results['artists']['items']:
#             for genre in artist['genres']:
#                 if genre in item['genres']:
#                     if artist['artist_name'].lower() == item['name'].lower():
#                         coll.find_one_and_update({'artist_name': artist['artist_name']},
#                                                  {
#                                                      '$set': {
#                                                          'spotify_id': item['id'],
#                                                          'spotify_genres': item['genres']
#                                                      }
#                                                  })
                        
#                         return 1
#                     else:
#                         break
#                 else:
#                     continue
#             if artist['artist_name'].lower() == item['name'].lower():
#                 coll.find_one_and_update({'artist_name': artist['artist_name']},
#                                          {
#                                              '$set': {
#                                                  'spotify_id': item['id'],
#                                                  'spotify_genres': item['genres']
#                                              }
#                                          })
#                 return 1
#             else:
#                 continue
#         return 0

def find_attr(tag):
    if tag.has_attr('content'):
        return True
    else:
        return False

def find_bc_release(bc_url):
    r = requests.get(bc_url)
    release = {}
    soup = BeautifulSoup(r.text, 'html.parser')
    album_id = soup.find('li', class_='music-grid-item')
    if album_id:
        embed_data = album_id.get('data-item-id')
        if 'track' in embed_data:
            release['type'] = 'track'
            release['id'] = embed_data.split('-')[1]
        elif 'album' in embed_data:
            release['type'] = 'album'
            release['id'] = embed_data.split('-')[1]
        return release
        
    else:
        if soup.find('meta',{'content':'song'}):
            release['type'] = 'track'
            meta_tags = soup.find_all('meta',{'name':'bc-page-properties'})
            page_props = list(filter(find_attr, meta_tags))
            json_object = json.loads(page_props[0].get('content'))
            release['id'] = str(json_object['item_id'])
        elif soup.find('meta',{'content':'album'}):
            release['type'] = 'album'
            meta_tags = soup.find_all('meta',{'name':'bc-page-properties'})
            page_props = list(filter(find_attr, meta_tags))
            json_object = json.loads(page_props[0].get('content'))
            release['id'] = str(json_object['item_id'])
        return release


n = 0
for artist in tqdm(artists):
#     update_status = spotify_update(artist)
    latest_release = find_bc_release(artist['bc_url'])
    coll.find_one_and_update({'artist_name': artist['artist_name']},
                            {'$set': {
                                'latest_release': latest_release
                            }})
    n+=1
    if n == 20:
        time.sleep(1)