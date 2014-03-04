import json
import urllib2
import time
import math
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING


def parseJson(url):
    data = json.load(urllib2.urlopen(url))
    return data


def write(post):
    client = MongoClient()
    db = client.weather

    posts = db.posts
    posts.ensure_index([("name", ASCENDING), ("day", ASCENDING)], unique=True, dropDups=True)
    try:
        post_id = posts.insert(post)
        print post_id
    except Exception:
        print "Duplicate key"
        return False
    return True

url = "http://api.openweathermap.org/data/2.5/weather?q=London,uk"
post = parseJson(url)
post['day'] = math.floor(time.time() % 86400)
write(post)