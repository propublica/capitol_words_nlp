import pymongo
import sys

c = pymongo.MongoClient()
c.drop_database(sys.argv[1])
