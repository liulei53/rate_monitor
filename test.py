from pymongo import MongoClient
class BinanceFundingRateTracker:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["funding_monitor"]
        self.users_collection = self.db["users"]

    def clear_user_database(self):
        """清空用户数据库中的所有记录"""
        result = self.users_collection.delete_many({})
        print(f"清空了 {result.deleted_count} 条用户数据。")

# 示例：清空数据库
if __name__ == "__main__":
    tracker = BinanceFundingRateTracker()
    tracker.clear_user_database()