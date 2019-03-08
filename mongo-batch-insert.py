from pymongo import MongoClient
from os import listdir
from os.path import isfile, join
import json
import time
from multiprocessing import Pool
import sys,random
uri = "mongodb://localhost"

client = MongoClient(uri)
db = client.samples
samples = db.samples

samples_path = "data"
list_dir = listdir(samples_path)

limit = 400
data = []
results = []
batches = []

for f in list_dir:
    fpath = join(samples_path, f)
    if isfile(fpath):
        d = json.load(open(fpath, "r"))
        d["partition"] = random.randint(1, 10)
        data.append(d)
        if len(data)==limit:
            s = sys.getsizeof(json.dumps(data))
            try:
                tick = time.time()
                res = samples.insert_many(data)
                results.append(res)
            except Exception as e:
                print(str(e))
            tock = time.time()
            time.sleep(1)
            print("batch (size:{}) time taken {}, total payload (MB)   {}, avg size per document (KB)  {} ".format(str(len(data)),str(tock-tick),str(s/1024/1024),str(s/1024/len(data))))

            data = []