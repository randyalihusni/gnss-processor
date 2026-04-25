#!/bin/bash
# update-gnss.sh — jalankan di SSH LXC Portainer
# Usage: bash /opt/gnss-processor/update-gnss.sh

REMOTE="/opt/gnss-processor"
CONTAINER="gnss_processor"

echo "=== GNSS Processor Update ==="

echo "[1/3] Copy file ke container..."
docker cp $REMOTE/app.py               $CONTAINER:/app/app.py
docker cp $REMOTE/batch_processor.py   $CONTAINER:/app/batch_processor.py
docker cp $REMOTE/crs_transform.py     $CONTAINER:/app/crs_transform.py
docker cp $REMOTE/igs_downloader.py    $CONTAINER:/app/igs_downloader.py
docker cp $REMOTE/job_store.py         $CONTAINER:/app/job_store.py
docker cp $REMOTE/lc_lw_analysis.py   $CONTAINER:/app/lc_lw_analysis.py
docker cp $REMOTE/report_generator.py  $CONTAINER:/app/report_generator.py
docker cp $REMOTE/templates/index.html $CONTAINER:/app/templates/index.html

echo "[2/3] Restart container..."
docker restart $CONTAINER

echo "[3/3] Verifikasi..."
sleep 3
docker exec $CONTAINER python3 -c "
from app import app
routes = sorted([r.rule for r in app.url_map.iter_rules()])
upload = [r for r in routes if 'upload' in r or 'jobs' in r or 'batch' in r]
print('Routes OK:', len(routes), 'total')
for r in upload: print(' ', r)
" 2>&1

echo ""
echo "=== DONE ==="
echo "Hard refresh browser: Ctrl+Shift+R"
