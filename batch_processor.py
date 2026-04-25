"""
batch_processor.py
==================
Batch processing multi-titik secara sekuensial.
Setiap file OBS diproses satu per satu dengan RTKLIB.

Output:
  - CSV summary semua titik
  - Shapefile (point) semua titik
  - PDF report gabungan
  - ZIP semua output
"""

import os
import csv
import json
import math
import struct
import zipfile
import threading
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Callable
from job_store import save_batch_jobs, job_age_str, is_recent

# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class BatchPoint:
    """Satu titik dalam batch."""
    idx:          int
    name:         str              # nama titik (dari nama file OBS)
    obs_file:     str              # path file OBS
    status:       str = 'queued'   # queued|downloading|processing|done|error
    progress:     int = 0          # 0-100
    job_id:       str = ''
    summary:      Optional[Dict] = None
    error:        str = ''
    nav_file:     Optional[str] = None
    sp3_file:     Optional[str] = None
    eph_messages: List[str] = field(default_factory=list)
    started_at:   str = ''
    finished_at:  str = ''
    ant_height:   str = '0'        # tinggi antena rover (m) per titik


@dataclass
class BatchJob:
    """Satu sesi batch processing."""
    batch_id:     str
    status:       str = 'queued'   # queued|running|done|error
    points:       List[BatchPoint] = field(default_factory=list)
    params:       Dict = field(default_factory=dict)
    ephemeris_type: str = 'broadcast'
    base_file:    Optional[str] = None
    created_at:   str = ''
    finished_at:  str = ''
    result_dir:   str = ''
    csv_file:     Optional[str] = None
    shp_file:     Optional[str] = None
    pdf_file:     Optional[str] = None
    zip_file:     Optional[str] = None


# ── Global batch store ────────────────────────────────────────────────────────
from job_store import load_batch_jobs as _load_batch
_persisted_batches = _load_batch()
batch_jobs: Dict[str, BatchJob] = {}
# Note: BatchJob objects tidak bisa di-deserialize langsung dari JSON,
# tapi status info tersimpan di batch_store.json untuk reference UI


# ── Point name from filename ──────────────────────────────────────────────────

def name_from_file(path: str) -> str:
    """Ekstrak nama titik dari nama file OBS."""
    base = os.path.basename(path)
    # Remove extension and common suffixes
    for ext in ['.obs','.rnx','.OBS','.RNX',
                '.21o','.22o','.23o','.24o','.25o','.26o',
                '.21O','.22O','.23O','.24O','.25O','.26O']:
        if base.endswith(ext):
            base = base[:-len(ext)]
            break
    # Remove trailing _R_, _S_, etc (RINEX 3 style)
    for suffix in ['_R_','_S_','_U_']:
        if suffix in base:
            base = base.split(suffix)[0]
    return base.upper() or 'POINT'


# ── Sequential batch runner ───────────────────────────────────────────────────

def run_batch(batch_id: str, jobs_store: dict,
              on_update: Optional[Callable] = None):
    """
    Jalankan batch processing secara sekuensial di background thread.

    Parameters
    ----------
    batch_id   : ID batch job
    jobs_store : dict jobs Flask (untuk create sub-jobs)
    on_update  : callback dipanggil setiap ada update status
    """
    batch = batch_jobs.get(batch_id)
    if not batch:
        return

    batch.status     = 'running'
    batch.created_at = datetime.now().isoformat()

    os.makedirs(batch.result_dir, exist_ok=True)
    eph_cache = os.path.join(batch.result_dir, 'eph_cache')
    os.makedirs(eph_cache, exist_ok=True)

    def notify():
        if on_update: on_update(batch_id)

    # ── Process each point sequentially ──────────────────────────────────────
    for pt in batch.points:
        pt.status     = 'downloading'
        pt.started_at = datetime.now().isoformat()
        notify()

        # 1. Download / locate ephemeris
        nav_file = batch.base_file  # reuse base if same day (handled below)
        sp3_file = None

        try:
            from igs_downloader import download_ephemeris, get_cached_ephemeris, parse_rinex_date

            # Check cache first
            date = parse_rinex_date(pt.obs_file)
            if date:
                year, month, day = date
                from igs_downloader import doy_from_date
                doy = doy_from_date(year, month, day)
                cached = get_cached_ephemeris(year, doy, eph_cache, batch.ephemeris_type)
                if cached.success:
                    nav_file = cached.nav_file or nav_file
                    sp3_file = cached.sp3_file
                    pt.eph_messages.append(f'Cache hit: DOY {doy}')
                else:
                    # Download
                    def eph_cb(msg):
                        pt.eph_messages.append(msg)
                        notify()

                    eph = download_ephemeris(
                        pt.obs_file, eph_cache,
                        batch.ephemeris_type,
                        callback=eph_cb,
                        timeout_total=30   # max 30s per file
                    )
                    nav_file = eph.nav_file or nav_file
                    sp3_file = eph.sp3_file
                    if not eph.success:
                        pt.eph_messages.append('Download gagal — menggunakan nav dari upload jika ada')
            else:
                pt.eph_messages.append('Tanggal tidak terbaca dari OBS — skip auto-download')

        except Exception as e:
            pt.eph_messages.append(f'Eph error: {e}')

        # Wajib ada nav file
        if not nav_file:
            timeout_hint = ' (container tidak bisa akses internet — gunakan Upload Manual)'                 if any('timeout' in m.lower() or 'internet' in m.lower() 
                       for m in pt.eph_messages) else ''
            pt.status = 'error'
            pt.error  = f'Navigation file tidak tersedia{timeout_hint}'
            pt.finished_at = datetime.now().isoformat()
            notify()
            continue

        pt.nav_file = nav_file
        pt.sp3_file = sp3_file
        pt.status   = 'processing'
        pt.progress = 10
        notify()

        # 2. Run RTKLIB
        try:
            import subprocess, uuid
            from app import build_conf, parse_pos_file, gen_demo

            jid = uuid.uuid4().hex[:8]
            pt.job_id = jid

            pt_result_dir = os.path.join(batch.result_dir, f'pt_{pt.idx:03d}_{pt.name}')
            os.makedirs(pt_result_dir, exist_ok=True)
            result_pos  = os.path.join(pt_result_dir, 'result.pos')
            conf_path   = os.path.join(pt_result_dir, 'rtklib.conf')

            # Merge batch params + point-specific
            params = dict(batch.params)
            params['mode'] = params.get('mode', 'static')
            # Override ant_ht_rover dengan nilai per titik
            if pt.ant_height and pt.ant_height != '0':
                params['ant_ht_rover'] = pt.ant_height

            build_conf(params, conf_path)

            cmd = ['rnx2rtkp', '-k', conf_path, '-o', result_pos, pt.obs_file]
            if batch.base_file and os.path.exists(batch.base_file):
                cmd.append(batch.base_file)
            cmd.append(nav_file)
            if sp3_file and os.path.exists(sp3_file):
                cmd.append(sp3_file)

            pt.progress = 30
            notify()

            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            pt.progress = 80
            notify()

            if proc.returncode == 0 and os.path.exists(result_pos):
                summary = parse_pos_file(result_pos)
                if summary:
                    pt.summary = summary
                    pt.summary['result_file'] = result_pos
                    pt.status = 'done'
                else:
                    pt.status = 'error'
                    pt.error  = 'File .pos kosong atau tidak bisa diparsing'
            elif proc.returncode == 127 or not os.path.exists('/usr/local/bin/rnx2rtkp'):
                # Demo mode
                pt.summary = gen_demo(params.get('mode','static'))
                pt.summary['point_name'] = pt.name
                pt.status = 'demo'
            else:
                pt.status = 'error'
                pt.error  = proc.stderr[:300] or f'RTKLIB exit code {proc.returncode}'

        except subprocess.TimeoutExpired:
            pt.status = 'error'
            pt.error  = 'Timeout >600s'
        except Exception as e:
            pt.status = 'error'
            pt.error  = str(e)

        pt.progress    = 100
        pt.finished_at = datetime.now().isoformat()
        notify()

    # ── Generate outputs ──────────────────────────────────────────────────────
    batch.status = 'generating'
    notify()

    done_points = [p for p in batch.points if p.status in ('done','demo') and p.summary]

    if done_points:
        try:
            batch.csv_file = _write_csv(batch, done_points)
        except Exception as e:
            pass

        try:
            batch.shp_file = _write_shapefile(batch, done_points)
        except Exception as e:
            pass

        try:
            batch.pdf_file = _write_batch_pdf(batch, done_points)
        except Exception as e:
            pass

        try:
            batch.zip_file = _write_zip(batch)
        except Exception as e:
            pass

    batch.status      = 'done'
    batch.finished_at = datetime.now().isoformat()
    # Persist batch job ke disk
    try:
        slim = {}
        for bid, b in batch_jobs.items():
            slim[bid] = {
                'batch_id': b.batch_id, 'status': b.status,
                'created_at': b.created_at, 'finished_at': b.finished_at,
                'n_points': len(b.points), 'ephemeris_type': b.ephemeris_type,
                'csv_file': b.csv_file, 'pdf_file': b.pdf_file,
                'shp_file': b.shp_file, 'zip_file': b.zip_file,
                'points': [{'idx':p.idx,'name':p.name,'status':p.status,
                            'fix_ratio': (p.summary or {}).get('fix_ratio',0),
                            'cep_mm': (p.summary or {}).get('cep_mm',0),
                            'error': p.error} for p in b.points]
            }
        save_batch_jobs(slim)
    except Exception:
        pass
    notify()


# ── CSV output ────────────────────────────────────────────────────────────────

def _write_csv(batch: BatchJob, points: List[BatchPoint]) -> str:
    path = os.path.join(batch.result_dir, f'batch_{batch.batch_id}_summary.csv')

    # CRS conversions
    try:
        from crs_transform import convert_all
    except Exception:
        convert_all = None

    fieldnames = [
        'No', 'Point_Name', 'Status',
        'Latitude_deg', 'Longitude_deg', 'Height_m',
        'UTM_E', 'UTM_N', 'UTM_Zone',
        'TM3_E', 'TM3_N', 'TM3_Zone',
        'Lat_Std_m', 'Lon_Std_m', 'Ht_Std_m',
        'CEP_mm', 'RMS_H_mm', 'RMS_V_mm',
        'Fix_Ratio_pct', 'Fixed_Epochs', 'Total_Epochs',
        'Processed_At',
    ]

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, pt in enumerate(points, 1):
            s = pt.summary or {}
            lat = s.get('lat_mean', 0)
            lon = s.get('lon_mean', 0)
            h   = s.get('height_mean', 0)

            # CRS
            utm_e = utm_n = utm_zone = tm3_e = tm3_n = tm3_zone = ''
            if convert_all and lat and lon:
                try:
                    crs_list = convert_all(lat, lon, h)
                    for crs in crs_list:
                        if 'UTM' in crs.short and abs(lon - (
                            int(crs.short.replace('UTM-','').replace('S',''))-1)*6-180-3) < 3:
                            utm_e    = crs.coords.get('Easting (m)', '')
                            utm_n    = crs.coords.get('Northing (m)', '')
                            utm_zone = crs.short
                            break
                    for crs in crs_list:
                        if 'TM3' in crs.short:
                            tm3_e    = crs.coords.get('Easting (m)', '')
                            tm3_n    = crs.coords.get('Northing (m)', '')
                            tm3_zone = crs.name
                            break
                except Exception:
                    pass

            writer.writerow({
                'No':            i,
                'Point_Name':    pt.name,
                'Status':        pt.status,
                'Latitude_deg':  f'{lat:.8f}',
                'Longitude_deg': f'{lon:.8f}',
                'Height_m':      f'{h:.4f}',
                'UTM_E':         f'{utm_e:.3f}' if isinstance(utm_e, float) else utm_e,
                'UTM_N':         f'{utm_n:.3f}' if isinstance(utm_n, float) else utm_n,
                'UTM_Zone':      utm_zone,
                'TM3_E':         f'{tm3_e:.3f}' if isinstance(tm3_e, float) else tm3_e,
                'TM3_N':         f'{tm3_n:.3f}' if isinstance(tm3_n, float) else tm3_n,
                'TM3_Zone':      tm3_zone,
                'Lat_Std_m':     f'{s.get("lat_std",0):.5f}',
                'Lon_Std_m':     f'{s.get("lon_std",0):.5f}',
                'Ht_Std_m':      f'{s.get("ht_std",0):.5f}',
                'CEP_mm':        f'{s.get("cep_mm",0):.2f}',
                'RMS_H_mm':      f'{s.get("rms_h",0):.2f}',
                'RMS_V_mm':      f'{s.get("rms_v",0):.2f}',
                'Fix_Ratio_pct': f'{s.get("fix_ratio",0):.1f}',
                'Fixed_Epochs':  s.get('fixed_epochs', 0),
                'Total_Epochs':  s.get('total_epochs', 0),
                'Processed_At':  pt.finished_at[:19],
            })

    return path


# ── Shapefile output (pure Python, no gdal) ───────────────────────────────────

def _write_shapefile(batch: BatchJob, points: List[BatchPoint]) -> str:
    """
    Tulis shapefile point (WGS-84) menggunakan pure Python.
    Format: .shp + .shx + .dbf + .prj
    """
    base = os.path.join(batch.result_dir, f'batch_{batch.batch_id}_points')

    # .prj — WGS84
    with open(base + '.prj', 'w') as f:
        f.write('GEOGCS["GCS_WGS_1984",'
                'DATUM["D_WGS_1984",'
                'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
                'PRIMEM["Greenwich",0.0],'
                'UNIT["Degree",0.0174532925199433]]')

    n = len(points)

    # Compute bounding box
    lons = [p.summary.get('lon_mean',0) for p in points]
    lats = [p.summary.get('lat_mean',0) for p in points]
    bbox = (min(lons), min(lats), max(lons), max(lats))

    # .shp records
    shp_records = []
    for pt in points:
        s   = pt.summary or {}
        lon = s.get('lon_mean', 0)
        lat = s.get('lat_mean', 0)
        # Point record: type(4) + x(8) + y(8) = 20 bytes content
        rec = struct.pack('<idddd', 1, lon, lat, lon, lat)  # type=1 (point)
        # Actually point is just type + x + y = 20 bytes
        rec = struct.pack('<i', 1) + struct.pack('<d', lon) + struct.pack('<d', lat)
        shp_records.append(rec)

    # .shp file
    shp_header_len = 50  # words
    rec_len        = 10  # words per record (4+8+8 bytes = 20 bytes = 10 words)
    file_len       = shp_header_len + n * (4 + rec_len)  # in 16-bit words

    with open(base + '.shp', 'wb') as shp:
        # File header (big-endian)
        shp.write(struct.pack('>iiiiii', 9994, 0,0,0,0,0))
        shp.write(struct.pack('>i', file_len))
        # Version, shape type (little-endian)
        shp.write(struct.pack('<ii', 1000, 1))
        # Bounding box
        shp.write(struct.pack('<dddddd', bbox[0], bbox[1], bbox[2], bbox[3], 0.0, 0.0))
        # Records
        for i, rec in enumerate(shp_records, 1):
            shp.write(struct.pack('>ii', i, rec_len))  # record header big-endian
            shp.write(rec)

    # .shx file
    shx_len = shp_header_len + n * 4
    with open(base + '.shx', 'wb') as shx:
        shx.write(struct.pack('>iiiiii', 9994, 0,0,0,0,0))
        shx.write(struct.pack('>i', shx_len))
        shx.write(struct.pack('<ii', 1000, 1))
        shx.write(struct.pack('<dddddd', bbox[0], bbox[1], bbox[2], bbox[3], 0.0, 0.0))
        offset = shp_header_len
        for i in range(n):
            shx.write(struct.pack('>ii', offset, rec_len))
            offset += 4 + rec_len

    # .dbf file
    fields = [
        ('No',         'N', 5,  0),
        ('Name',       'C', 20, 0),
        ('Status',     'C', 10, 0),
        ('Lat_deg',    'N', 16, 8),
        ('Lon_deg',    'N', 16, 8),
        ('Height_m',   'N', 12, 4),
        ('FixRatio',   'N', 6,  1),
        ('CEP_mm',     'N', 8,  2),
        ('RMS_H_mm',   'N', 8,  2),
        ('RMS_V_mm',   'N', 8,  2),
        ('LatStd_m',   'N', 10, 5),
        ('LonStd_m',   'N', 10, 5),
        ('HtStd_m',    'N', 10, 5),
    ]

    header_size = 32 + len(fields)*32 + 1
    rec_size    = 1 + sum(f[2] for f in fields)

    with open(base + '.dbf', 'wb') as dbf:
        # DBF header
        now = datetime.now()
        dbf.write(struct.pack('BBBB', 3, now.year-1900, now.month, now.day))
        dbf.write(struct.pack('<I', n))
        dbf.write(struct.pack('<HH', header_size, rec_size))
        dbf.write(b'\x00' * 20)

        # Field descriptors
        for fname, ftype, flen, fdec in fields:
            name_b = fname.encode('ascii')[:11].ljust(11, b'\x00')
            dbf.write(name_b)
            dbf.write(ftype.encode('ascii'))
            dbf.write(b'\x00' * 4)
            dbf.write(struct.pack('BB', flen, fdec))
            dbf.write(b'\x00' * 14)
        dbf.write(b'\r')  # header terminator

        # Records
        for i, pt in enumerate(points, 1):
            s   = pt.summary or {}
            lat = s.get('lat_mean', 0)
            lon = s.get('lon_mean', 0)
            h   = s.get('height_mean', 0)
            dbf.write(b' ')  # deletion flag
            def nf(v, w, d): return f'{v:{w}.{d}f}'.encode('ascii')
            def cf(v, w):    return v[:w].ljust(w).encode('ascii')
            dbf.write(f'{i:5d}'.encode('ascii'))
            dbf.write(cf(pt.name, 20))
            dbf.write(cf(pt.status, 10))
            dbf.write(nf(lat, 16, 8))
            dbf.write(nf(lon, 16, 8))
            dbf.write(nf(h,   12, 4))
            dbf.write(nf(s.get('fix_ratio',0), 6, 1))
            dbf.write(nf(s.get('cep_mm',0),    8, 2))
            dbf.write(nf(s.get('rms_h',0),     8, 2))
            dbf.write(nf(s.get('rms_v',0),     8, 2))
            dbf.write(nf(s.get('lat_std',0),   10, 5))
            dbf.write(nf(s.get('lon_std',0),   10, 5))
            dbf.write(nf(s.get('ht_std',0),    10, 5))
        dbf.write(b'\x1a')  # EOF

    return base + '.shp'


# ── PDF report gabungan ───────────────────────────────────────────────────────

def _write_batch_pdf(batch: BatchJob, points: List[BatchPoint]) -> str:
    """Generate PDF report gabungan semua titik."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table,
        TableStyle, PageBreak, KeepTogether
    )
    from reportlab.lib.colors import HexColor

    C_DARK   = HexColor('#0a1628'); C_PANEL = HexColor('#0d2240')
    C_ACCENT = HexColor('#00c49a'); C_ACC2  = HexColor('#0077e6')
    C_LIGHT  = HexColor('#e8f2ff'); C_DIM   = HexColor('#4a6080')
    C_GOOD   = HexColor('#00b87a'); C_WARN  = HexColor('#e09500')
    C_BAD    = HexColor('#e04444'); C_TBLALT= HexColor('#f0f6ff')

    path = os.path.join(batch.result_dir, f'batch_{batch.batch_id}_report.pdf')
    W, H = A4; margin = 20*mm; cw = W - 2*margin

    doc = SimpleDocTemplate(path, pagesize=A4,
        leftMargin=margin, rightMargin=margin,
        topMargin=22*mm, bottomMargin=16*mm)

    def on_page(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(C_DARK); canvas.rect(0,H-18*mm,W,18*mm,fill=1,stroke=0)
        canvas.setFillColor(C_ACCENT); canvas.rect(0,H-18*mm,5,18*mm,fill=1,stroke=0)
        canvas.setFillColor(C_LIGHT); canvas.setFont('Courier-Bold',9)
        canvas.drawString(12*mm,H-9*mm,'GNSS BATCH PROCESSING REPORT')
        canvas.setFillColor(C_DIM); canvas.setFont('Courier',7)
        canvas.drawRightString(W-15*mm,H-9*mm,
            f"BATCH #{batch.batch_id}  |  {datetime.now().strftime('%Y-%m-%d')}")
        canvas.setFillColor(C_DARK); canvas.rect(0,0,W,12*mm,fill=1,stroke=0)
        canvas.setFillColor(C_ACCENT); canvas.rect(0,0,W,1.5,fill=1,stroke=0)
        canvas.setFillColor(C_DIM); canvas.setFont('Courier',7)
        canvas.drawString(15*mm,4*mm,'KJSB Randy dan Rekan — Geodetic Survey Division')
        canvas.drawRightString(W-15*mm,4*mm,f'Page {doc.page}')
        canvas.restoreState()

    sT = ParagraphStyle('T',fontName='Courier-Bold',fontSize=20,textColor=C_LIGHT,
                        spaceAfter=4,leading=26)
    sS = ParagraphStyle('S',fontName='Courier',fontSize=10,textColor=C_ACCENT,
                        spaceAfter=2,leading=14)
    sN = ParagraphStyle('N',fontName='Courier',fontSize=7.5,textColor=C_DIM,
                        spaceAfter=4,leading=11)

    def ts(extra=None):
        base = [
            ('FONTNAME',(0,0),(-1,-1),'Courier'),('FONTSIZE',(0,0),(-1,-1),7.5),
            ('FONTNAME',(0,0),(-1,0),'Courier-Bold'),
            ('BACKGROUND',(0,0),(-1,0),C_PANEL),('TEXTCOLOR',(0,0),(-1,0),C_ACCENT),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,C_TBLALT]),
            ('TEXTCOLOR',(0,1),(-1,-1),C_DARK),
            ('GRID',(0,0),(-1,-1),0.3,C_DIM),
            ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),
            ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ]
        if extra: base += extra
        return base

    story = []
    story.append(Spacer(1,8*mm))
    story.append(Paragraph('GNSS BATCH PROCESSING', sT))
    story.append(Paragraph(f'MULTI-POINT SURVEY REPORT — {len(points)} Titik', sS))
    story.append(Spacer(1,4*mm))

    # ── Summary table ─────────────────────────────────────────────────────────
    ok = sum(1 for p in points if p.status in ('done','demo'))
    err= sum(1 for p in points if p.status == 'error')

    meta = [
        ['Parameter','Value'],
        ['Batch ID',     f'#{batch.batch_id}'],
        ['Total Titik',  str(len(batch.points))],
        ['Berhasil',     str(ok)],
        ['Gagal',        str(err)],
        ['Ephemeris',    batch.ephemeris_type.title()],
        ['Mode',         batch.params.get('mode','static').title()],
        ['Processed At', batch.finished_at[:19] if batch.finished_at else '—'],
    ]
    mt = Table(meta, colWidths=[50*mm, cw-50*mm])
    mt.setStyle(TableStyle(ts()))
    story.append(mt)
    story.append(Spacer(1,5*mm))

    # ── All points coordinate table ───────────────────────────────────────────
    hdr = ['No','Nama Titik','Latitude (°)','Longitude (°)',
           'Height (m)','CEP (mm)','Fix Ratio','Status']
    rows = [hdr]
    for i, pt in enumerate(points, 1):
        s   = pt.summary or {}
        lat = s.get('lat_mean',0); lon = s.get('lon_mean',0)
        h   = s.get('height_mean',0)
        r   = s.get('fix_ratio',0)
        cep = s.get('cep_mm',0)
        rows.append([
            str(i), pt.name,
            f'{lat:.8f}', f'{lon:.8f}', f'{h:.4f}',
            f'{cep:.2f}', f'{r:.1f}%', pt.status.upper()
        ])

    tbl = Table(rows, colWidths=[10*mm,30*mm,38*mm,38*mm,22*mm,18*mm,18*mm,18*mm])
    ts_extra = []
    for i, pt in enumerate(points, 1):
        col = C_GOOD if pt.status in ('done','demo') else C_BAD
        ts_extra.append(('TEXTCOLOR',(7,i),(7,i),col))
        ts_extra.append(('FONTNAME', (7,i),(7,i),'Courier-Bold'))
        r = (pt.summary or {}).get('fix_ratio',0)
        rc = C_GOOD if r>=80 else C_WARN if r>=50 else C_BAD
        ts_extra.append(('TEXTCOLOR',(6,i),(6,i),rc))
    tbl.setStyle(TableStyle(ts(ts_extra)))
    story.append(KeepTogether([
        Paragraph('Ringkasan Koordinat Semua Titik',
                  ParagraphStyle('H',fontName='Courier-Bold',fontSize=9,
                                 textColor=C_ACCENT,spaceAfter=4,leading=12)),
        tbl
    ]))
    story.append(Spacer(1,5*mm))

    # ── Per-point detail pages ────────────────────────────────────────────────
    for pt in points:
        story.append(PageBreak())
        s = pt.summary or {}
        story.append(Paragraph(f'Detail: {pt.name}',
            ParagraphStyle('PH',fontName='Courier-Bold',fontSize=14,
                           textColor=C_LIGHT,spaceAfter=4,leading=18)))
        story.append(Paragraph(f'File: {os.path.basename(pt.obs_file)}  |  '
                               f'Status: {pt.status.upper()}  |  '
                               f'Selesai: {pt.finished_at[:19]}',
            ParagraphStyle('PS',fontName='Courier',fontSize=8,
                           textColor=C_DIM,spaceAfter=8,leading=12)))

        if pt.status == 'error':
            story.append(Paragraph(f'Error: {pt.error}',
                ParagraphStyle('E',fontName='Courier',fontSize=9,
                               textColor=C_BAD,spaceAfter=4)))
            continue

        # Coordinate detail
        coord_data = [
            ['Komponen','Nilai','Std Dev (m)'],
            ['Latitude (°)',  f'{s.get("lat_mean",0):.8f}',  f'±{s.get("lat_std",0):.5f}'],
            ['Longitude (°)', f'{s.get("lon_mean",0):.8f}',  f'±{s.get("lon_std",0):.5f}'],
            ['Height (m)',    f'{s.get("height_mean",0):.4f}',f'±{s.get("ht_std",0):.5f}'],
        ]
        ct = Table(coord_data, colWidths=[45*mm,70*mm,cw-115*mm])
        ct.setStyle(TableStyle(ts()))
        story.append(ct)
        story.append(Spacer(1,3*mm))

        # Quality
        qual_data = [
            ['Metrik','Nilai','Metrik','Nilai'],
            ['Total Epochs', f'{s.get("total_epochs",0):,}',
             'CEP 2D',       f'{s.get("cep_mm",0):.2f} mm'],
            ['Fixed (Q=1)',  f'{s.get("fixed_epochs",0):,}',
             'RMS H',        f'{s.get("rms_h",0):.2f} mm'],
            ['Fix Ratio',    f'{s.get("fix_ratio",0):.1f}%',
             'RMS V',        f'{s.get("rms_v",0):.2f} mm'],
        ]
        qt = Table(qual_data, colWidths=[40*mm,30*mm,40*mm,cw-110*mm])
        qt.setStyle(TableStyle(ts()))
        story.append(qt)
        story.append(Spacer(1,3*mm))

        # CRS output per titik
        try:
            from crs_transform import convert_all, results_to_dict
            crs_list = convert_all(
                s.get('lat_mean',0), s.get('lon_mean',0), s.get('height_mean',0))
            priority = [c for c in crs_list if c.epsg in [4326,32749,23830]]
            if priority:
                crs_rows = [['CRS','E / Lat','N / Lon','Height (m)']]
                for crs in priority:
                    vals = list(crs.coords.values())
                    v0 = f'{vals[0]:.8f}' if crs.unit=='degree' else f'{vals[0]:.3f}'
                    v1 = f'{vals[1]:.8f}' if crs.unit=='degree' else f'{vals[1]:.3f}'
                    v2 = f'{vals[2]:.4f}'
                    crs_rows.append([crs.name, v0, v1, v2])
                crs_tbl = Table(crs_rows, colWidths=[55*mm,42*mm,42*mm,cw-139*mm])
                crs_tbl.setStyle(TableStyle(ts()))
                story.append(Paragraph('CRS Output',
                    ParagraphStyle('CH',fontName='Courier-Bold',fontSize=8,
                                   textColor=C_ACCENT,spaceAfter=3,leading=11)))
                story.append(crs_tbl)
        except Exception:
            pass

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    return path


# ── ZIP output ────────────────────────────────────────────────────────────────

def _write_zip(batch: BatchJob) -> str:
    zip_path = os.path.join(batch.result_dir, f'batch_{batch.batch_id}_output.zip')
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for attr in ('csv_file','pdf_file'):
            f = getattr(batch, attr, None)
            if f and os.path.exists(f):
                zf.write(f, os.path.basename(f))

        # Shapefile components
        if batch.shp_file:
            base = batch.shp_file.replace('.shp','')
            for ext in ('.shp','.shx','.dbf','.prj'):
                fp = base + ext
                if os.path.exists(fp):
                    zf.write(fp, os.path.basename(fp))

        # Individual .pos files
        for pt in batch.points:
            if pt.summary and pt.summary.get('result_file'):
                pf = pt.summary['result_file']
                if os.path.exists(pf):
                    zf.write(pf, f'{pt.name}/{os.path.basename(pf)}')

    return zip_path
