GNSS Processor v3 — RTKLIB Web App
====================================
Oleh: KJSB Randy dan Rekan

CARA INSTALL DI LXC PORTAINER (192.168.18.34):
------------------------------------------------
1. Upload seluruh folder ini ke LXC via WinSCP
   Target: /opt/gnss-processor/

2. Buka Console LXC di Proxmox web UI, jalankan:
   cd /opt/gnss-processor
   docker build -t gnss-processor:latest .

3. Cek nama network NPM:
   docker network ls | grep -i npm

4. Edit docker-compose.yml — ganti nama network NPM
   (baris: name: npm_default)

5. Buka Portainer → Stacks → Add Stack
   Paste isi docker-compose.yml → Deploy

6. Setup NPM Proxy Host:
   Domain  : gnss.kjsbrandydanrekan.com
   Forward : gnss_processor : 5050
   Websockets: ON

STRUKTUR FILE:
--------------
gnss-processor/
├── app.py                  Flask backend utama
├── lc_lw_analysis.py       Modul analisis LC/LW
├── report_generator.py     Generator report PDF
├── requirements.txt        Python dependencies
├── Dockerfile              Build image Docker
├── docker-compose.yml      Portainer Stack config
├── templates/
│   └── index.html          UI web app
├── uploads/                Upload RINEX (auto-created)
└── results/                Hasil processing (auto-created)

ENDPOINT:
---------
/                   UI utama
/process            POST — upload & proses RINEX
/status/<id>        GET  — status job
/download/<id>      GET  — download .pos file
/config/<id>        GET  — download RTKLIB config
/report/<id>        GET  — download report PDF
/jobs               GET  — list semua job
