FROM python:3.11-slim
WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y \
    gcc make wget unzip \
    libfreetype6-dev libpng-dev \
    fonts-dejavu-core \
    rtklib \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Buat folder runtime
RUN mkdir -p uploads results

EXPOSE 5050
CMD ["python", "app.py"]
