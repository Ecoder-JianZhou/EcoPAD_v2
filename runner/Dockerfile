FROM python:3.11-slim

WORKDIR /app

# install python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy code
COPY app ./app
COPY config ./config

# create db folder
RUN mkdir -p /app/db

ENV RUNNER_DB_PATH=/app/db/runner.db
ENV SITES_CONFIG_PATH=/app/config/sites.json

EXPOSE 8001

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]