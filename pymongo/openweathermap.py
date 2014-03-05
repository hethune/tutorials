import json
import urllib2
import time
import math
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING

def debug(info):
    print info

def log(info):
    print info

def parseJson(url):
    try:
        data = json.load(urllib2.urlopen(url))
        return data
    except ValueError as e:
        log(e)
        exit()
    except:
        log("Url Error: " + url)
        exit()

def openDBCollection(database, collectionName):
    client = MongoClient()
    db = client[database]
    collection = db[collectionName]
    # In case we need to make results unique
    # collection.ensure_index([("name", ASCENDING), ("start", ASCENDING)], unique=True, dropDups=True)
    return collection

def validateData(raw):
    data = [];
    for key in raw:
        value = raw[key]
        if isinstance(value, basestring) and value.lower() == "error":
            log("Failed retrieve latency for " + key)
        else:
            value["name"] = key
            data.append(value)
    return data

def write(collection, posts):
    for post in posts:
        try:
            post_id = collection.insert(post)
            debug(post_id)
        except Exception:
            log("Insertion failed for" + post["name"])
    return True

def main(url):
    # url = "http://stackoverflow.com/questions/1479776/too-many-values-to-unpack-exception"
    data = parseJson(url)
    posts = validateData(data)
    collection = openDBCollection('latency', 'dmos')
    write(collection, posts)

url = "http://api.openweathermap.org/data/2.5/weather?q=London,uk"
main(url)


