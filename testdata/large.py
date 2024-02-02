import json
from db import DB

db = DB("data.db")

with open("testdata/generated.json") as f:
    data_list = json.load(f)
    print(f"loading {len(data_list)} datas")
    for data in data_list:
        db.create_group(data["_id"])
        db.create_data(data["_id"], "config.json")
        db.write_data(data["_id"], "config.json", json.dumps(data).encode())
