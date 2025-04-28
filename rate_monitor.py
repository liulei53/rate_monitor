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

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


load_dotenv()

class BinanceFundingRateTracker:
    # 初始化方法 加载本地json数据文件
    def __init__(self):
        self.current_rates = {}  # 当前费率
        self.previous_rates = {}  # 初始化 previous_rates，避免属性不存在
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["funding_monitor"]
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

    def safe_request(self, url, params=None, timeout=10):
        """封装请求，支持自动重试"""
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"⚠️ 请求失败: {e}")
            return None

    def fetch_latest_stats(self):
        stats_collection = self.db["funding_rate_stats"]
        return stats_collection.find_one(
            {"exchange": "binance", "interval": "8h"},
            sort=[("timestamp", -1)]
        )

    def fetch_recent_alerts(self, limit=10):
        alerts_collection = self.db["funding_alerts"]
        return alerts_collection.find().sort("timestamp", -1).limit(limit)

    def fetch_latest_sentiment(self):
        sentiment_collection = self.db["market_sentiment"]
        return sentiment_collection.find_one(sort=[("timestamp", -1)])       



    def handle_message(self, message_text: str):
        try:
            if message_text in ["📈 资金费率最高（Top10）", "📉 资金费率最低（Top10）", "📈 排行榜（高费率）"]:
                latest = self.fetch_latest_stats()
                if not latest:
                    return "暂无数据，请稍后重试。"

                if message_text == "📈 资金费率最高（Top10）":
                    items = latest.get("top_highest", [])
                    title = "📈 当前资金费率最高排行（Top 10）"
                    symbol_format = "🔥 {symbol}: {rate:.5%}"

                elif message_text == "📉 资金费率最低（Top10）":
                    items = latest.get("top_lowest", [])
                    title = "📉 当前资金费率最低排行（Top 10）"
                    symbol_format = "❄️ {symbol}: {rate:.5%}"

                elif message_text == "📈 排行榜（高费率）":
                    items = [i for i in latest.get("top_highest", []) if i.get("rate", 0) > 0.01]
                    if not items:
                        return "暂无异常高费率"
                    title = "📈 多头费率高的币种"
                    symbol_format = "🔥 {symbol}: {rate:.5%}"

                lines = [title]
                for item in items:
                    lines.append('-' * 35)
                    lines.append(symbol_format.format(symbol=item['symbol'], rate=item['rate']))
                return "\n".join(lines)

            elif message_text == "📣 最近告警记录":
                alerts = self.fetch_recent_alerts()
                lines = ["📣 最近告警记录（最多10条）"]
                for alert in alerts:
                    time_str = alert["timestamp"].strftime('%m-%d %H:%M')
                    if alert["type"] == "extreme":
                        lines.append(f"🔥[{time_str}] {alert['symbol']} 费率异常: {alert['rate']:.5%}")
                    elif alert["type"] == "change":
                        lines.append(f"⚡[{time_str}] {alert['symbol']} 变化剧烈: Δ{alert['change']:+.5%}")
                return "\n".join(lines) if len(lines) > 1 else "暂无最近告警"

            elif message_text == "🕓 上次检查时间":
                return f"🕓 最近一次更新时间为：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

            elif message_text == "🔄 立即刷新":
                self.run_task()
                return "✅ 已完成一次刷新任务。"

            elif message_text == "📊 市场情绪指数":
                latest = self.fetch_latest_sentiment()
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

                return (f"📊 市场情绪指数（{time_str}）\n\n"
                        f"得分：{score} 分（{mood}）\n"
                        f"全市场平均资金费率：{avg_rate:.5%}")

            elif message_text == "⚡ 资金变化最快（Top10）":
                stats_collection = self.db["funding_rate_stats"]
                cursor = stats_collection.find(
                    {"exchange": "binance", "interval": "8h"}
                ).sort("timestamp", -1).limit(2)
                records = list(cursor)
                if len(records) < 2:
                    return "📊 数据不足，无法比较变化，请等待更多更新。"

                current_rates = {i["symbol"]: i.get("rate", i.get("change", 0)) for i in records[0].get("top_highest", []) + records[0].get("top_lowest", [])}
                previous_rates = {i["symbol"]: i.get("rate", i.get("change", 0)) for i in records[1].get("top_highest", []) + records[1].get("top_lowest", [])}

                changes = {symbol: current_rates[symbol] - previous_rates[symbol]
                           for symbol in current_rates if symbol in previous_rates}

                top_changes = sorted(changes.items(), key=lambda x: abs(x[1]), reverse=True)[:10]

                lines = ["⚡ 最近资金变化最快的 Top 10"]
                for symbol, change in top_changes:
                    lines.append('-' * 35)
                    lines.append(f"⚡ {symbol}: 变化 {change:+.5%}")
                return "\n".join(lines)

            elif message_text == "📊 热门合约榜（高费率 + 高成交额）":
                result = self.get_funding_rate_with_volume(rate_threshold=0.005, volume_threshold=10_000_000)
                if not result:
                    return "暂无满足条件的币种。"

                lines = ["📊 热门合约榜（资金费率高 + 成交额大）\n"]
                for idx, item in enumerate(result[:10], 1):
                    oi_rate_str = f"{item['oi_change_rate']:+.2%}" if item['oi_change_rate'] is not None else "N/A"
                    oi_val_str = f"{item['open_interest']:,.0f}" if item['open_interest'] is not None else "N/A"
                    lines.append(f"{idx}. {item['symbol']}")
                    lines.append(f"   📈 资金费率: {item['funding_rate']:+.5%}")
                    lines.append(f"   💰 成交额: ${item['volume_24h']:,.2f}")
                    lines.append(f"   📦 OI: {oi_val_str}")
                    lines.append(f"   🔁 OI变化率: {oi_rate_str}")
                return "\n".join(lines)

            else:
                return "🤖 未识别的指令，请通过菜单选择操作。"

        except Exception as e:
            return f"⚠️ 查询失败：{e}"

    # 获取所有USDT结尾的永续合约交易对
    def get_usdt_perpetual_symbols(self) -> List[str]:
        """获取所有USDT结尾的永续合约交易对"""
        try:
            response = self.safe_request("https://fapi.binance.com/fapi/v1/exchangeInfo")
            if not response:
                return []
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
    def get_funding_rates(self) -> Dict[str, float]:
        """获取所有USDT交易对的资金费率"""
        try:
            response = self.safe_request("https://fapi.binance.com/fapi/v1/premiumIndex")
            if not response:
                return {}
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
            response = self.safe_request("https://fapi.binance.com/fapi/v1/ticker/24hr")
            if not response:
                return []
            tickers = response.json()
            volumes = {item['symbol']: float(item['quoteVolume']) for item in tickers}

            # 获取最新的 OI 数据
            client = MongoClient("mongodb://localhost:27017/")
            db = client["funding_monitor"]
            oi_collection = db["open_interest"]
            latest_oi = oi_collection.find_one(sort=[("timestamp", -1)])
            if not latest_oi:
                print("❌ 无 OI 数据，无法筛选")
                return []
            oi_data = latest_oi["oi_data"]

            result = []
            for symbol, rate in rates.items():
                volume = volumes.get(symbol)
                oi_info = oi_data.get(symbol, {})
                oi = oi_info.get("open_interest")
                oi_change_rate = oi_info.get("oi_change_rate", None)  # 如果字段不存在，设置为 None
                if volume and abs(rate) >= rate_threshold and volume >= volume_threshold:
                    result.append({
                        "symbol": symbol,
                        "funding_rate": rate,
                        "volume_24h": volume,
                        "open_interest": oi,
                        "oi_change_rate": oi_change_rate
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
            
    def get_top_n(self, rates: Dict[str, float], n: int, reverse: bool = True) -> List[Tuple[str, float]]:
        """获取费率最高/最低的n个交易对"""
        sorted_rates = sorted(rates.items(), key=lambda x: x[1], reverse=reverse)
        return sorted_rates[:n]
    
    # 获取费率变化最大的n个交易对
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

    def fetch_all_market_data(self) -> Dict[str, float]:
        """拉取当前市场资金费率"""
        rates = self.get_funding_rates()
        print(f"当前共获取 {len(rates)} 个币种的资金费率")
        return rates

    def store_raw_data(self, rates: Dict[str, float], timestamp: str):
        """将原始资金费率写入 MongoDB"""
        try:
            client = MongoClient("mongodb://localhost:27017/")
            db = client["funding_monitor"]
            funding_rates_collection = db["funding_rates"]
            funding_rates_collection.insert_one({
                "timestamp": datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'),
                "exchange": "binance",
                "interval": "8h",
                "rates": rates
            })
            print("已写入 funding_rates 原始数据")
        except Exception as e:
            print(f"Error saving to funding_rates: {e}")

    def calculate_top_stats(self, rates: Dict[str, float], timestamp: str):
        """根据当前资金费率生成统计数据并存储"""
        highest_rates = self.get_top_n(rates, 10, reverse=True)
        lowest_rates = self.get_top_n(rates, 10, reverse=False)
        increasing_rates = []
        decreasing_rates = []

        if self.previous_rates:
            increasing_rates = self.get_biggest_changes(rates, self.previous_rates, 10, increasing=True)
            decreasing_rates = self.get_biggest_changes(rates, self.previous_rates, 10, increasing=False)

        try:
            client = MongoClient("mongodb://localhost:27017/")
            db = client["funding_monitor"]
            stats_collection = db["funding_rate_stats"]
            stats_collection.insert_one({
                "timestamp": datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'),
                "exchange": "binance",
                "interval": "8h",
                "top_highest": [{"symbol": s, "rate": r} for s, r in highest_rates],
                "top_lowest": [{"symbol": s, "rate": r} for s, r in lowest_rates],
                "top_increases": [{"symbol": s, "change": c} for s, c in increasing_rates],
                "top_decreases": [{"symbol": s, "change": c} for s, c in decreasing_rates]
            })
            print("已写入 funding_rate_stats 统计数据")
        except Exception as e:
            print(f"Error saving to funding_rate_stats: {e}")

    def get_shanghai_timestamp(self) -> str:
        """获取当前上海时区时间字符串"""
        tz = pytz.timezone("Asia/Shanghai")
        return datetime.now(pytz.utc).astimezone(tz).strftime('%Y-%m-%d %H:%M:%S')

    def update_previous_rates(self):
        """更新上一轮资金费率快照"""
        self.previous_rates = self.current_rates.copy()
    
    #获取指定交易对当前未平仓合约量（Open Interest
    def get_open_interest(self, symbol: str) -> Optional[float]:
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        params = {"symbol": symbol}
        response = self.safe_request(url, params=params)
        if response:
            data = response.json()
            return float(data.get("openInterest"))
        return None
    
    def fetch_and_store_open_interest(self, timestamp: str):
        """获取所有交易对的 OI 数据并存入数据库"""
        try:
            symbols = self.get_usdt_perpetual_symbols()
            oi_data = {}

             # 从数据库中获取上一轮的 OI 数据
            client = MongoClient("mongodb://localhost:27017/")
            db = client["funding_monitor"]
            oi_collection = db["open_interest"]
            latest_oi = oi_collection.find_one(sort=[("timestamp", -1)])
            previous_oi_data = latest_oi["oi_data"] if latest_oi else {}

            for symbol in symbols:
                current_oi = self.get_open_interest(symbol)
                if current_oi is not None:
                    # 获取上一轮的 OI 数据
                    previous_oi = previous_oi_data.get(symbol, {}).get("open_interest", None)
                    if previous_oi is not None and previous_oi != 0:
                        oi_change_rate = (current_oi - previous_oi) / previous_oi
                    else:
                        oi_change_rate = None  # 如果没有上一轮数据或为 0，则无法计算变化率

                    # 存储当前 OI 和变化率
                    oi_data[symbol] = {
                        "open_interest": current_oi,
                        "oi_change_rate": oi_change_rate
                    }
                else:
                    print(f"⚠️ {symbol} 没有返回 OI 数据，可能不是永续合约")
    
            # 存入 MongoDB
            oi_collection.insert_one({
                "timestamp": datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'),
                "exchange": "binance",
                "oi_data": oi_data
            })
            print(f"已存储 {len(oi_data)} 个交易对的 OI 数据")
            return oi_data
        except Exception as e:
            print(f"Error fetching and storing OI data: {e}")
            return {}    
        
    def get_high_impact_symbols(self, rate_threshold=0.005, volume_threshold=10_000_000, oi_threshold=1_000_000):
        """筛选资金费率高、成交量大、OI 高的交易对"""
        try:
            # 获取资金费率和成交量
            rates = self.get_funding_rates()
            response = self.safe_request("https://fapi.binance.com/fapi/v1/ticker/24hr")
            if not response:
                return []
            tickers = response.json()
            volumes = {item['symbol']: float(item['quoteVolume']) for item in tickers}
    
            # 获取最新的 OI 数据
            client = MongoClient("mongodb://localhost:27017/")
            db = client["funding_monitor"]
            oi_collection = db["open_interest"]
            latest_oi = oi_collection.find_one(sort=[("timestamp", -1)])
            if not latest_oi:
                print("❌ 无 OI 数据，无法筛选")
                return []
            oi_data = latest_oi["oi_data"]
    
            # 筛选符合条件的交易对
            result = []
            for symbol, rate in rates.items():
                volume = volumes.get(symbol)
                oi = oi_data.get(symbol)
                if volume and oi and abs(rate) >= rate_threshold and volume >= volume_threshold and oi >= oi_threshold:
                    result.append({
                        "symbol": symbol,
                        "funding_rate": rate,
                        "volume_24h": volume,
                        "open_interest": oi
                    })
    
            # 按资金费率绝对值排序
            result.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
            return result
        except Exception as e:
            print(f"Error getting high impact symbols: {e}")
            return []   
        
    # 获取当前上海时间戳
    def run_task(self):
        if not self.telegram_bot.token or not self.telegram_chat_id:
            raise ValueError("Telegram bot token or chat ID is not set.")
        
        timestamp = self.get_shanghai_timestamp()
        print(f"🕒 Running task at {timestamp}")

        self.current_rates = self.fetch_all_market_data()
        if not self.current_rates:
            print("❌ 获取资金费率失败，跳过本轮任务")
            return

        self.store_raw_data(self.current_rates, timestamp)
        self.fetch_and_store_open_interest(timestamp)
        self.calculate_top_stats(self.current_rates, timestamp)
        self.update_previous_rates()
        self.check_and_send_alerts(timestamp)
        self.calculate_sentiment_index(timestamp)
        self.check_and_send_alerts(timestamp)

    def check_and_send_alerts(self, timestamp: str):
        """检查资金费率异常并发送Telegram告警"""
        try:
            client = MongoClient("mongodb://localhost:27017/")
            db = client["funding_monitor"]
            alerts_collection = db["funding_alerts"]

            alert_lines = [f"🚨 **资金费率预警**（{timestamp}）"]
            abnormal_rates = []
            violent_changes = []
            all_extreme_rates = []
            now_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

            for symbol, rate in self.current_rates.items():
                previous = self.previous_rates.get(symbol)
                change = rate - previous if previous is not None else 0
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
                        kline_response = self.safe_request(f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=30")
                        if not kline_response:
                            continue
                        kline_data = kline_response.json()
                        if isinstance(kline_data, list) and len(kline_data) >= 2:
                            open_price = float(kline_data[0][1])
                            close_price = float(kline_data[-1][4])
                            price_change = (close_price - open_price) / open_price
                            if price_change > 0.01:
                                abnormal_rates.append(f"❗ {symbol} 空头费率极低 {rate:.5%}，但价格近30分钟上涨 {price_change:.2%}，可能为诱多或逼空走势，请注意风险！")
                            else:
                                abnormal_rates.append(f"❄️ {symbol} 空头费率极低 {rate:.5%}，或有反弹机会！")
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

    def calculate_sentiment_index(self, timestamp: str):
        """计算市场情绪指数并存储"""
        try:
            import numpy as np
            rates = list(self.current_rates.values())
            if rates:
                avg_rate = np.mean(rates)
                std_rate = np.std(rates)
                z = avg_rate / (std_rate + 1e-6)
                sentiment_score = round(100 / (1 + np.exp(-10 * z)), 2)
                client = MongoClient("mongodb://localhost:27017/")
                db = client["funding_monitor"]
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