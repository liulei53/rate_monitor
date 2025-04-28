from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["funding_monitor"]
oi_collection = db["open_interest"]

# 清空集合
oi_collection.delete_many({})
print("已清空 open_interest 集合")