FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gossip.py server.py ./
ENV USE_UDP=1
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
