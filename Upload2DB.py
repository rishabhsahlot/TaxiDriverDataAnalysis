import pandas as pd
from pymongo import MongoClient
import os

processed_data_path = 'Processed_Data'
# Creates a client connection to the default MongoDB databse as 27017
client = MongoClient()
# Estabilishing a connection with an already existing database named taxi_data
db = client['taxi_data']

directory = processed_data_path

for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        df = pd.read_csv(directory + "/" + filename)
        print(df.columns)
        coll = db[filename[:-4]]
        df.reset_index(inplace=True)
        df.rename(columns={df.columns[0]: '_id'}, inplace=True)
        df = df.to_dict("records")
        coll.insert_many(df)
        del df
    else:
        continue
