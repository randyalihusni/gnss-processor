FROM python:3.11-slim

WORKDIR /app

# System deps untuk matplotlib (font rendering) dan build tools
RUN apt-get update && apt-get install -y \
    gcc make wget unzip \
    libfreetype6-dev libpng-dev \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Install RTKLIB rnx2rtkp dari source
# Jika gagal build, app tetap jalan dalam demo mode
RUN wget -q https://github.com/tomojitakasu/RTKLIB/archive/refs/tags/v2.4.3b34.tar.gz \
    && tar xzf v2.4.3b34.tar.gz \
    && cd RTKLIB-2.4.3b34/app/rnx2rtkp/gcc \
    && make -s \
    && cp rnx2rtkp /usr/local/bin/ \
    && cd /app \
    && rm -rf RTKLIB* v2.4.3b34.tar.gz \
    && echo "RTKLIB installed OK" \
    || echo "RTKLIB build skipped — running in demo mode"

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Buat folder runtime
RUN mkdir -p uploads results

EXPOSE 5050

CMD ["python", "app.py"]
