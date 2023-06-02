import pymongo 
import pandas as pd

# read the data in pandas
df = pd.read_csv('data/Twitter_Data.csv').head(2) # reading just 2000 files for the PoC
df.reset_index(level=0, inplace=True)
comments = df.to_dict(orient='records')

# # connect to the mongodb client
myclient = pymongo.MongoClient("mongodb+srv://choprahetarth:45AJpXuKlK90Xc5s@cluster0.jcnnsrz.mongodb.net/?retryWrites=true&w=majority")

# # # make a new database
mydb = myclient["twitter_collection"]
# # # with a new column
mycol = mydb["comments"]

# # add the clean text column to the mongodb
mycol.insert_many(comments)