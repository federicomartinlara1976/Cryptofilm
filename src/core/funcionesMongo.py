# Funciones
from pymongo import MongoClient

from core import cfg as c


def getMongoClient():
    return MongoClient(c.cfg['mongo.server'])


def getDb(client):
    return client[c.cfg['mongo.database']]


def getCollection(db, cname):
    return db[cname]


def getDatabase():
    client = getMongoClient()
    return getDb(client)
