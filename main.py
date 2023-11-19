import csv
import json
import re
from pymongo import MongoClient
from bson import json_util
from pymongo.collection import Collection
from pymongo.cursor import Cursor


def checkNull(val: str) -> bool:
    null_vals = ["null", "nil", "Null", "NIL"]
    return val in null_vals


def replaceNullVals(entry: list) -> list:
    for i in range(len(entry)):
        if checkNull(entry[i]):
            entry[i] = ""
    return entry


def loadDataToDB(fileName: str, dbName: str) -> None:
    with MongoClient(host="localhost", port=27017) as client:
        db = client[dbName]
        attraction = db[dbName]

        with open(fileName) as file:
            csvreader = csv.reader(file, delimiter=",")
            fields_ = []
            idx = 1
            for row in csvreader:
                if idx == 1:
                    fields_ = row.copy()
                    idx += 1
                    continue

                entry = dict(zip(fields_, replaceNullVals(row)))
                attraction.insert_one(entry)
                idx += 1


def exportToJson(client: MongoClient, dbName: str, fileName: str) -> None:
    db = client[dbName]
    collection = db[dbName]
    cursor = collection.find({})
    json.dump(json_util.dumps(cursor),
              open(fileName, "w"))


def importFromJson(client: MongoClient, dbName: str, fileName: str) -> None:
    db = client[dbName]

    if dbName in db.list_collection_names():
        return

    collection = db[dbName]
    with open(fileName) as file:
        bsondata = json.load(file)
    
    jsondata = re.sub(r'ObjectId\s*\(\s*\"(\S+)\"\s*\)',
                      r'{"$oid": "\1"}',
                      bsondata)
    data = json.loads(jsondata, object_hook=json_util.object_hook)

    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)


def getAllData(c: Collection) -> Cursor:
    entries = c.find({})
    return entries


def getCountDocuments(c: Collection) -> int:
    return c.count_documents({})


def printData(c, cnt=-1) -> None:
    if cnt >= 0:
        for row in c[:cnt]:
            print(row)
    else:
        for row in c:
            print(row)


with MongoClient(host="localhost", port=27017) as client:
    loadDataToDB("tourist_attractions.csv", "tourist_attractions")

    db = client["tourist_attractions"]
    collection = db["tourist_attractions"]

    # get and print all documents in the db
    printData(getAllData(collection))

    # get and print the number of documents in the db
    print(getCountDocuments(collection))

    # get and print all regions
    region_cursor = collection.find({}, {"region": 1})
    print("#"*5 + " Regions " + "#"*5)
    for region in region_cursor:
        print(region["region"])

    # get and print documents that contain
    # 'region': 'St. Petersburg' or 'type': 'architecture'
    result = collection.find({"$or":
                     [
                         {"region": "St. Petersburg"},
                         {"type": "architecture"}
                     ]
                     }) 
    for entry in result:
        print(entry)
    
    # update the value of the "region" field from
    # "St. Petersburg" to "Saint Petersburg"
    collection.update_many({"region": "St. Petersburg"},
                      {"$set": {"region": "Saint Petersburg"}})
