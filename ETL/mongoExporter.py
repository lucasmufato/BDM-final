from sys import argv

import pandas as pd
from pymongo import MongoClient

url, port, database, path = argv[1], argv[2], argv[3], argv[4]

client = MongoClient(url, int(port))
print("Connection Successful")

db = client.database

print("Database created")

df = pd.read_csv(path)

print("csv read")
coll = db.create_collection("songs_metadata")
print("collection created")
coll.insert_many(df.to_dict('records'))

print("inserts done")
client.close()
print("done")