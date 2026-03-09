# Sử dụng image Python nhẹ nhàng
FROM python:3.11-slim

# workd dir setup
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
