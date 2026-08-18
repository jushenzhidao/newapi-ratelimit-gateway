FROM python:3.13-slim

WORKDIR /app

# 安装 uv（固定版本，保证构建可复现）
COPY --from=ghcr.io/astral-sh/uv:0.11.14 /uv /uv/bin/uv
ENV PATH="/uv/bin:$PATH"

# 先复制依赖声明，利用 Docker 分层缓存（pyproject.toml/uv.lock 不变时跳过安装）
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# 复制代码
COPY . .

EXPOSE 8080

# 启动（--no-sync 复用构建期已同步的 .venv；host/port/workers 由环境变量控制）
CMD ["sh", "-c", "exec uv run --no-sync uvicorn app.main:app --host ${SERVER_HOST:-0.0.0.0} --port ${SERVER_PORT:-8080} --workers ${SERVER_WORKERS:-4}"]
