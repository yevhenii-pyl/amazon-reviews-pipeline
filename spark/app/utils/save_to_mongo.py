# spark/app/save_to_mongo.py

from pymongo import MongoClient

def save_df_to_mongo(df, mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Drop collection to avoid duplicates (for dev/demo only)
    collection.drop()

    # Convert to dictionaries and insert
    records = [row.asDict() for row in df.collect()]
    if records:
        collection.insert_many(records)
    print(f"Inserted {len(records)} records into {collection_name}.")

    client.close()
