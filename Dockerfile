FROM python:3.11-slim

WORKDIR /app

# Installa dipendenze
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia proxy
COPY proxy_atm_cloud.py .

# Espone porta (Fly.io usa 8080 di default, ma possiamo usare 8888)
EXPOSE 8888

# Avvia il proxy
CMD ["python3", "proxy_atm_cloud.py"]
