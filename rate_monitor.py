import requests
import json
import time
import os
from datetime import datetime
import schedule
from typing import Dict, List, Tuple, Optional
import pytz  # 添加 pytz 库
from pymongo import MongoClient
import telegram
from dotenv import load_dotenv
from telegram.ext import Updater, MessageHandler, Filters
from telegram import ReplyKeyboardMarkup


load_dotenv()

class BinanceFundingRateTracker:
    # 初始化方法 加载本地json数据文件
    def __init__(self):
        self.current_rates = {}  # 当前费率
        self.previous_rates = {}  # 初始化 previous_rates，避免属性不存在

        # 初始化 Telegram Bot
        self.telegram_bot = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")


        # 启动 telegram 消息监听（用于处理菜单按钮点击）
        self.updater = Updater(token=self.telegram_bot.token, use_context=True)
        dispatcher = self.updater.dispatcher

        def respond(update, context):
            response = self.handle_message(update.message.text)
            context.bot.send_message(chat_id=update.effective_chat.id, text=response)

        dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), respond))
        self.updater.start_polling()

    def handle_message(self, message_text: str):
        """
        根据按钮点击内容返回相应信息
        """
        if message_text == "📈 资金费率最高（Top10）":
            try:
                client = MongoClient("mongodb://localhost:27017/")
                db = client["funding_monitor"]
                stats_collection = db["funding_rate_stats"]

                latest = stats_collection.find_one(
                    {"exchange": "binance", "interval": "8h"},
                    sort=[("timestamp", -1)]
                )

                if not latest or "top_highest" not in latest:
                    return "暂无数据，请稍后重试。"

                lines = ["📈 当前资金费率最高排行（Top 10）"]
                for item in latest["top_highest"]:
                    lines.append('-' * 35)
                    lines.append(f"🔥 {item['symbol']}: {item['rate']:.5%}")
                return "\n".join(lines)
            except Exception as e:
                return f"⚠️ 查询失败：{e}"

        elif message_text == "📉 资金费率最低（Top10）":
            try:
                client = MongoClient("mongodb://localhost:27017/")
                db = client["funding_monitor"]
                stats_collection = db["funding_rate_stats"]

                latest = stats_collection.find_one(
                    {"exchange": "binance", "interval": "8h"},
                    sort=[("timestamp", -1)]
                )

                if not latest or "top_lowest" not in latest:
                    return "暂无数据，请稍后重试。"

                lines = ["📉 当前资金费率最低排行（Top 10）"]
                for item in latest["top_lowest"]:
                    lines.append('-' * 35)
                    lines.append(f"❄️ {item['symbol']}: {item['rate']:.5%}")
                return "\n".join(lines)
            except Exception as e:
                return f"⚠️ 查询失败：{e}"

        elif message_text == "📈 排行榜（高费率）":
            try:
                client = MongoClient("mongodb://localhost:27017/")
                db = client["funding_monitor"]
                stats_collection = db["funding_rate_stats"]

                latest = stats_collection.find_one(
                    {"exchange": "binance", "interval": "8h"},
                    sort=[("timestamp", -1)]
                )

                if not latest or "top_highest" not in latest:
                    return "暂无数据，请稍后重试。"

                lines = ["📈 多头费率高的币种"]
                for item in latest["top_highest"]:
                    if item["rate"] > 0.01:
                        lines.append(f"🔥 {item['symbol']}: {item['rate']:.5%}")
                return "\n".join(lines) if len(lines) > 1 else "暂无异常高费率"
            except Exception as e:
                return f"⚠️ 查询失败：{e}"

        elif message_text == "📣 最近告警记录":
            try:
                client = MongoClient("mongodb://localhost:27017/")
                db = client["funding_monitor"]
                alerts_collection = db["funding_alerts"]

                recent_alerts = alerts_collection.find().sort("timestamp", -1).limit(10)
                lines = ["📣 最近告警记录（最多10条）"]
                for alert in recent_alerts:
                    time_str = alert["timestamp"].strftime('%m-%d %H:%M')
                    if alert["type"] == "extreme":
                        lines.append(f"🔥[{time_str}] {alert['symbol']} 费率异常: {alert['rate']:.5%}")
                    elif alert["type"] == "change":
                        lines.append(f"⚡[{time_str}] {alert['symbol']} 变化剧烈: Δ{alert['change']:+.5%}")
                return "\n".join(lines) if len(lines) > 1 else "暂无最近告警"
            except Exception as e:
                return f"⚠️ 查询失败：{e}"
        
        elif message_text == "🕓 上次检查时间":
            return f"🕓 最近一次更新时间为：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        elif message_text == "🔄 立即刷新":
            self.run_task()
            return "✅ 已完成一次刷新任务。"

        elif message_text == "📊 市场情绪指数":
            try:
                client = MongoClient("mongodb://localhost:27017/")
                db = client["funding_monitor"]
                sentiment_collection = db["market_sentiment"]

                latest = sentiment_collection.find_one(sort=[("timestamp", -1)])
                if not latest:
                    return "暂无市场情绪数据。"

                score = latest["score"]
                avg_rate = latest["avg_rate"]
                time_str = latest["timestamp"].strftime('%Y-%m-%d %H:%M')

                if score < 20:
                    mood = "🥶 极度恐慌"
                elif score < 40:
                    mood = "😟 偏空观望"
                elif score < 60:
                    mood = "😐 中性平稳"
                elif score < 80:
                    mood = "😊 偏多乐观"
                else:
                    mood = "🚀 极度贪婪"

                return f"""📊 市场情绪指数（{time_str}）

得分：{score} 分（{mood}）
全市场平均资金费率：{avg_rate:.5%}
"""
            except Exception as e:
                return f"⚠️ 查询失败：{e}"

        elif message_text == "📉 资金费率最低（Top10）":
            lines = ["📉 当前资金费率最低排行（Top 10）"]
            top = self.get_top_n(self.current_rates, 10, reverse=False)
            for symbol, rate in top:
                lines.append('-' * 35)
                lines.append(f"❄️ {symbol}: {rate:.5%}")
            return "\n".join(lines)
        
        elif message_text == "⚡ 资金变化最快（Top10）":
            lines = ["⚡ 最近资金费率变化最快的 Top 10"]
            try:
                client = MongoClient("mongodb://localhost:27017/")
                db = client["funding_monitor"]
                stats_collection = db["funding_rate_stats"]

                # 获取最近两条记录
                cursor = stats_collection.find(
                    {"exchange": "binance", "interval": "8h"}
                ).sort("timestamp", -1).limit(2)
                records = list(cursor)
                if len(records) < 2:
                    return "📊 数据不足，无法比较变化，请等待更多更新。"

                current_rates = {item["symbol"]: item.get("rate", item.get("change", 0)) for item in records[0].get("top_highest", []) + records[0].get("top_lowest", [])}
                previous_rates = {item["symbol"]: item.get("rate", item.get("change", 0)) for item in records[1].get("top_highest", []) + records[1].get("top_lowest", [])}

                changes = {}
                for symbol in current_rates:
                    if symbol in previous_rates:
                        change = current_rates[symbol] - previous_rates[symbol]
                        changes[symbol] = change

                top_changes = sorted(changes.items(), key=lambda x: abs(x[1]), reverse=True)[:10]

                for symbol, change in top_changes:
                    lines.append('-' * 35)
                    lines.append(f"⚡ {symbol}: 变化 {change:+.5%}")
                return "\n".join(lines)
            except Exception as e:
                return f"⚠️ 数据查询失败：{e}"
        elif message_text == "📊 热门合约榜（高费率 + 高成交额）":
            lines = ["📊 热门合约榜（资金费率高 + 成交额大）\n"]
            try:
                result = self.get_funding_rate_with_volume(rate_threshold=0.005, volume_threshold=10_000_000)
                if not result:
                    return "暂无满足条件的币种。"

                for idx, item in enumerate(result[:10], 1):
                    lines.append(f"{idx}. {item['symbol']}")
                    lines.append(f"   📈 资金费率: {item['funding_rate']:+.5%}")
                    lines.append(f"   💰 成交额: ${item['volume_24h']:,.2f}")
                return "\n".join(lines)
            except Exception as e:
                return f"⚠️ 查询失败：{e}"
        else:
            return "🤖 未识别的指令，请通过菜单选择操作。"

    # 获取所有USDT结尾的永续合约交易对
    # 该方法从Binance API获取所有USDT结尾的永续合约交易对
    # 该方法返回一个包含所有USDT结尾的永续合约交易对的列表
    # 该方法使用requests库发送HTTP GET请求到Binance API
    def get_usdt_perpetual_symbols(self) -> List[str]:
        """获取所有USDT结尾的永续合约交易对"""
        try:
            response = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
            data = response.json()

            usdt_symbols = []
            for symbol_info in data['symbols']:
                if symbol_info['symbol'].endswith('USDT') and symbol_info['status'] == 'TRADING' and symbol_info[
                    'contractType'] == 'PERPETUAL':
                    usdt_symbols.append(symbol_info['symbol'])

            return usdt_symbols
        except Exception as e:
            print(f"Error fetching symbols: {e}")
            return []
    # 获取所有USDT交易对的资金费率
    # 该方法从Binance API获取所有USDT交易对的资金费率
    # 该方法返回一个包含所有USDT交易对的资金费率的字典
    # 该方法使用requests库发送HTTP GET请求到Binance API
    def get_funding_rates(self) -> Dict[str, float]:
        """获取所有USDT交易对的资金费率"""
        try:
            response = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex")
            data = response.json()

            funding_rates = {}
            for item in data:
                symbol = item['symbol']
                if symbol.endswith('USDT'):
                    funding_rate = float(item['lastFundingRate'])
                    funding_rates[symbol] = funding_rate

            return funding_rates
        except Exception as e:
            print(f"Error fetching funding rates: {e}")
            return {}

    def get_funding_rate_with_volume(self, rate_threshold=0.005, volume_threshold=10_000_000) -> List[Dict]:
        """获取资金费率与成交量均较高的币种"""
        try:
            # 获取资金费率
            rates = self.get_funding_rates()

            # 获取24小时成交额
            response = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr")
            tickers = response.json()
            volumes = {item['symbol']: float(item['quoteVolume']) for item in tickers}

            result = []
            for symbol, rate in rates.items():
                volume = volumes.get(symbol)
                if volume and abs(rate) >= rate_threshold and volume >= volume_threshold:
                    result.append({
                        "symbol": symbol,
                        "funding_rate": rate,
                        "volume_24h": volume
                    })

            # 排序：先按资金费率绝对值，再按成交额降序
            result.sort(key=lambda x: (abs(x["funding_rate"]), x["volume_24h"]), reverse=True)
            print("符合条件的高费率+高成交额币种：")
            for item in result:
                print(f"{item['symbol']}: 费率 {item['funding_rate']:.5%}, 成交额 ${item['volume_24h']:.2f}")
            return result

        except Exception as e:
            print(f"Error getting funding rate with volume: {e}")
            return []
    #   获取费率最高/最低的n个交易对
    # 该方法接受一个字典和一个整数n作为参数
    # 该方法返回一个包含n个交易对的列表
    # 该方法使用sorted函数对字典进行排序
    # 该方法使用lambda函数作为key参数
    # 该方法使用reverse参数来决定排序的顺序
    # 该方法使用切片操作来获取前n个交易对
    # 该方法使用列表推导式来创建一个包含n个交易对的列表
    # 该方法使用元组来存储交易对的名称和费率
    # 该方法使用类型注解来指定参数和返回值的类型
    # 该方法使用typing模块中的Dict和List类型
    # 该方法使用typing模块中的Tuple类型
    def get_top_n(self, rates: Dict[str, float], n: int, reverse: bool = True) -> List[Tuple[str, float]]:
        """获取费率最高/最低的n个交易对"""
        sorted_rates = sorted(rates.items(), key=lambda x: x[1], reverse=reverse)
        return sorted_rates[:n]
    # 获取费率变化最大的n个交易对
    # 该方法接受两个字典和一个整数n作为参数
    # 该方法返回一个包含n个交易对的列表
    # 该方法使用for循环遍历字典
    # 该方法使用if语句来判断费率的变化
    # 该方法使用lambda函数作为key参数
    # 该方法使用sorted函数对字典进行排序
    # 该方法使用切片操作来获取前n个交易对
    def get_biggest_changes(self, current: Dict[str, float], previous: Dict[str, float], n: int,
                            increasing: bool = True) -> List[Tuple[str, float]]:
        """获取费率变化最大的n个交易对"""
        changes = {}
        for symbol, rate in current.items():
            if symbol in previous:
                change = rate - previous[symbol]
                if (increasing and change > 0) or (not increasing and change < 0):
                    changes[symbol] = change

        sorted_changes = sorted(changes.items(), key=lambda x: x[1], reverse=increasing)
        return sorted_changes[:n]

    def run_task(self):
        if not self.telegram_bot.token or not self.telegram_chat_id:
            raise ValueError("Telegram bot token or chat ID is not set. Please configure environment variables.")
        """执行主要任务"""
        # 获取 Asia/Shanghai 时区
        shanghai_tz = pytz.timezone("Asia/Shanghai")
        
        # 将当前时间转换为 Asia/Shanghai 时间
        timestamp = datetime.now(pytz.utc).astimezone(shanghai_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Running task at {timestamp}")

        # 获取当前所有USDT交易对的资金费率
        self.current_rates = self.get_funding_rates()
        print(f"当前共获取 {len(self.current_rates)} 个币种的资金费率")
                
        # 插入原始资金费率数据到 funding_rates 集合
        try:
            client = MongoClient("mongodb://localhost:27017/")
            db = client["funding_monitor"]
            funding_rates_collection = db["funding_rates"]
            funding_rates_collection.insert_one({
                "timestamp": datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'),
                "exchange": "binance",
                "interval": "8h",
                "rates": self.current_rates
            })
            print("已写入 funding_rates 原始数据")
        except Exception as e:
            print(f"Error saving to funding_rates: {e}")

        if not self.current_rates:
            print("Failed to get funding rates, skipping this run")
            return

        # 统计1: 费率最高的10个symbol
        highest_rates = self.get_top_n(self.current_rates, 10, reverse=True)

        # 统计2: 费率最低的10个symbol
        lowest_rates = self.get_top_n(self.current_rates, 10, reverse=False)

        # 统计3 & 4: 费率变化最大的交易对
        increasing_rates = []
        decreasing_rates = []

        if self.previous_rates:
            # 统计3: 费率上升最大的10个symbol
            increasing_rates = self.get_biggest_changes(self.current_rates, self.previous_rates, 10, increasing=True)

            # 统计4: 费率下降最大的10个symbol
            decreasing_rates = self.get_biggest_changes(self.current_rates, self.previous_rates, 10, increasing=False)

        # 准备保存的数据
        stats = {
            "timestamp": timestamp,  # 使用 Asia/Shanghai 时间
            "highest_rates": [{"symbol": s, "rate": r} for s, r in highest_rates],
            "lowest_rates": [{"symbol": s, "rate": r} for s, r in lowest_rates],
            "biggest_increases": [{"symbol": s, "change": c} for s, c in increasing_rates],
            "biggest_decreases": [{"symbol": s, "change": c} for s, c in decreasing_rates],
            "previous_rates": self.current_rates  # 保存当前费率作为下次比较的基准
        }

                # 插入统计结果到 funding_rate_stats 集合
        try:
            stats_collection = db["funding_rate_stats"]
            stats_collection.insert_one({
                "timestamp": datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'),
                "exchange": "binance",
                "interval": "8h",
                "top_highest": stats["highest_rates"],
                "top_lowest": stats["lowest_rates"],
                "top_increases": stats["biggest_increases"],
                "top_decreases": stats["biggest_decreases"]
            })
            print("已写入 funding_rate_stats 统计数据")
        except Exception as e:
            print(f"Error saving to funding_rate_stats: {e}")


        # 更新previous_rates为当前rates，以便下次比较
        self.previous_rates = self.current_rates.copy()
        print("已更新 previous_rates，准备下次对比")

        # Telegram 报警逻辑
        try:
            client = MongoClient("mongodb://localhost:27017/")
            db = client["funding_monitor"]
            alerts_collection = db["funding_alerts"]
 
            alert_lines = [f"🚨 **资金费率预警**（{timestamp}）"]
 
            # 告警条件集合
            abnormal_rates = []
            violent_changes = []
            all_extreme_rates = []
            now_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
 
            for symbol, rate in self.current_rates.items():
                previous = self.previous_rates.get(symbol)
                change = rate - previous if previous is not None else 0
 
                # MongoDB 去重逻辑（1小时内不重复告警）
                recent_alert = alerts_collection.find_one({
                    "symbol": symbol,
                    "type": {"$in": ["extreme", "change"]},
                    "timestamp": {"$gte": now_dt.replace(minute=now_dt.minute - 60 if now_dt.minute >= 60 else 0)}
                })

 
                if rate > 0.01 and not recent_alert:
                    abnormal_rates.append(f"🔥 {symbol} 多头费率高达 {rate:.5%}，注意回调风险！")
                    alerts_collection.insert_one({
                        "symbol": symbol,
                        "type": "extreme",
                        "rate": rate,
                        "timestamp": now_dt
                    })
                elif rate < -0.01 and not recent_alert:
                    try:
                        kline_url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=30"
                        kline_response = requests.get(kline_url)
                        kline_data = kline_response.json()
                        if isinstance(kline_data, list) and len(kline_data) >= 2:
                            open_price = float(kline_data[0][1])
                            close_price = float(kline_data[-1][4])
                            price_change = (close_price - open_price) / open_price

                            if price_change > 0.01:
                                abnormal_rates.append(
                                    f"❗ {symbol} 空头费率极低 {rate:.5%}，但价格近30分钟上涨 {price_change:.2%}，可能为诱多或逼空走势，请注意风险！"
                                )
                            else:
                                abnormal_rates.append(
                                    f"❄️ {symbol} 空头费率极低 {rate:.5%}，或有反弹机会！"
                                )
                    except Exception as ex:
                        print(f"Error fetching kline for {symbol}: {ex}")

                    alerts_collection.insert_one({
                        "symbol": symbol,
                        "type": "extreme",
                        "rate": rate,
                        "timestamp": now_dt
                    })
 
                if abs(change) > 0.005 and not recent_alert:
                    violent_changes.append(f"⚡ {symbol} 资金费率剧烈变化 Δ{change:+.5%}")
                    alerts_collection.insert_one({
                        "symbol": symbol,
                        "type": "change",
                        "change": change,
                        "rate": rate,
                        "timestamp": now_dt
                    })
 
                if abs(rate) > 0.01:
                    all_extreme_rates.append(symbol)
 
            # 市场级异动判断
            if len(all_extreme_rates) >= 10:
                alert_lines.append(f"📊 当前有 {len(all_extreme_rates)} 个币种资金费率异常（> ±1%）📈 市场情绪偏激，请注意风险！")
 
            if abnormal_rates:
                alert_lines.append("\n🌡 **极端资金费率**")
                alert_lines.extend(abnormal_rates)
 
            if violent_changes:
                alert_lines.append("\n💥 **剧烈波动提醒**")
                alert_lines.extend(violent_changes)
 
            if len(alert_lines) > 1:
                message = "\n".join(alert_lines)
                self.telegram_bot.send_message(chat_id=self.telegram_chat_id, text=message, parse_mode="Markdown")
        except Exception as e:
            print(f"Telegram alert failed: {e}")
        print("完成 Telegram 异常告警检查")

        # 计算市场情绪指数（基于平均费率和标准差）
        try:
            import numpy as np
            rates = list(self.current_rates.values())
            if rates:
                avg_rate = np.mean(rates)
                std_rate = np.std(rates)
                z = avg_rate / (std_rate + 1e-6)
                sentiment_score = round(100 / (1 + np.exp(-10 * z)), 2)

                db["market_sentiment"].insert_one({
                    "timestamp": datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'),
                    "avg_rate": avg_rate,
                    "std_rate": std_rate,
                    "score": sentiment_score
                })
                print(f"Market Sentiment Score: {sentiment_score}")
        except Exception as e:
            print(f"Error calculating market sentiment: {e}")


if __name__ == "__main__":
    tracker = BinanceFundingRateTracker()
    
    # 发送自定义菜单按钮（提前）
    keyboard = [
        ['📈 资金费率最高（Top10）', '📉 资金费率最低（Top10）'],
        ['📣 最近告警记录', '📊 市场情绪指数'],
        ['⚡ 资金变化最快（Top10）', '📊 热门合约榜（高费率 + 高成交额）']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    try:
        tracker.telegram_bot.send_message(
            chat_id=tracker.telegram_chat_id,
            text="🤖 请选择一个操作 👇",
            reply_markup=reply_markup
        )
    except Exception as e:
        print(f"Telegram menu message failed: {e}")
    
    # 立即运行一次
    tracker.run_task()

    # 每5分钟运行一次
    schedule.every(5).minutes.do(tracker.run_task)

    print("Funding rate tracker started. Press Ctrl+C to exit.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Tracker stopped by user.")