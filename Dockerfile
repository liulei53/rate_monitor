# 使用官方 Python 作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制项目文件到容器内
COPY . /app

# 创建并激活虚拟环境
RUN python -m venv /venv

# 激活虚拟环境并安装依赖
RUN /venv/bin/pip install --upgrade pip && \
    /venv/bin/pip install -r requirements.txt

# 设置环境变量，确保程序可以找到虚拟环境
ENV PATH="/venv/bin:$PATH"

# 容器启动时执行 rate_monitor.py
CMD ["python", "rate_monitor.py"]