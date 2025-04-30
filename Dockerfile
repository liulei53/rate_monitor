# 使用官方 Python 镜像
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 复制项目所有文件
COPY . /app

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 设置 Python 输出不缓冲
ENV PYTHONUNBUFFERED=1

# 默认启动程序
CMD ["python", "rate_monitor.py"]