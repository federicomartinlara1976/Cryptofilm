# Funciones
from pymongo import MongoClient

import cfg as cfg


def getMongoClient():
    return MongoClient(cfg.cfg['mongo.server'])


def getDb(client):
    return client[cfg.cfg['mongo.database']]


def getCollection(db):
    return db[cfg.cfg['mongo.collection']]
