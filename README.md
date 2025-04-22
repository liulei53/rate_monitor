# Binance Funding Rate Tracker

🚀 一个基于 Python + MongoDB + Telegram Bot 的币安资金费率监控系统。

该程序每 5 分钟自动采集币安永续合约资金费率数据，进行分析、排名、告警，并通过 Telegram 推送异常情况与市场情绪指数。

---

## 📦 功能概览

### ✅ 数据采集与存储
- 获取币安 USDT 永续合约资金费率
- 每 5 分钟自动运行
- 数据写入 MongoDB：
  - `funding_rates`：原始数据
  - `funding_rate_stats`：Top10榜单 + 变动分析
  - `funding_alerts`：异常告警记录
  - `market_sentiment`：市场情绪指数

---

### 🤖 Telegram Bot 菜单功能

| 按钮 | 功能说明 |
|------|----------|
| 📈 资金费率最高（Top10） | 当前资金费率最高的合约 |
| 📉 资金费率最低（Top10） | 当前资金费率最低的合约 |
| ⚡ 资金变化最快（Top10） | 相比上一次变化最大的前 10 个币 |
| 📣 最近告警记录 | 展示最近 10 条触发的告警 |
| 📊 市场情绪指数 | 综合分析市场情绪的评分（0～100） |
| 📊 热门合约榜（高费率 + 高成交额） | 同时满足高资金费率和高成交额的热门币种 |

---

### ⚠️ 告警系统

自动检测并推送以下异常情况：
- 极端资金费率（> ±1%）
- 剧烈变动（单轮变化 > ±0.5%）
- 市场级别异动（10+ 币种同时异常）

每个币种 1 小时内只告警一次，避免重复刷屏。

---

## 🛠️ 项目部署

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

或使用 requirements.txt 一键安装：

```bash
pip install -r requirements.txt
```

---

## requirements.txt

```
python-telegram-bot==13.15
pymongo
schedule
python-dotenv
requests
numpy
```