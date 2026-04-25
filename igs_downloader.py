"""
igs_downloader.py
=================
Auto-download GNSS ephemeris dari IGS mirror servers.

Mendukung:
  - Broadcast navigation (RINEX 3 mixed: GPS+GLO+GAL+BDS)
  - Precise orbit SP3 (IGS Final, Rapid, Ultra-Rapid)
  - Precise clock CLK (IGS Final, Rapid)

Mirror servers (diurutkan dari yang terdekat ke Indonesia):
  1. KASI (Korea)   — nfs.kasi.re.kr
  2. BKG  (Germany) — igs.bkg.bund.de
  3. CDDIS (NASA)   — cddis.nasa.gov (anonymous FTP)

File naming (IGS RINEX 3 convention):
  Broadcast : BRDM00DLR_S_YYYYDDD0000_01D_MN.rnx.gz
  SP3 Final : IGS0OPSFIN_YYYYDDD0000_01D_15M_ORB.SP3.gz
  SP3 Rapid : IGS0OPSRAP_YYYYDDD0000_01D_15M_ORB.SP3.gz
  CLK Final : IGS0OPSFIN_YYYYDDD0000_01D_30S_CLK.CLK.gz
  CLK Rapid : IGS0OPSRAP_YYYYDDD0000_01D_05M_CLK.CLK.gz
"""

import os
import gzip
import shutil
import ftplib
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
import logging

logger = logging.getLogger(__name__)

# ── GPS time utilities ────────────────────────────────────────────────────────

def doy_from_date(year: int, month: int, day: int) -> int:
    """Day of Year (DOY) dari tanggal."""
    return datetime(year, month, day).timetuple().tm_yday


def gps_week_dow(year: int, month: int, day: int) -> Tuple[int, int]:
    """GPS week number dan day-of-week dari tanggal."""
    gps_epoch = datetime(1980, 1, 6, tzinfo=timezone.utc)
    dt = datetime(year, month, day, tzinfo=timezone.utc)
    delta = dt - gps_epoch
    week = delta.days // 7
    dow  = delta.days % 7
    return week, dow


def parse_rinex_date(obs_file: str) -> Optional[Tuple[int, int, int]]:
    """
    Baca tanggal pertama epoch dari file RINEX OBS.
    Return (year, month, day) atau None jika gagal.
    """
    try:
        with open(obs_file, 'r', errors='ignore') as f:
            in_header = True
            for line in f:
                # Cari end of header
                if 'END OF HEADER' in line:
                    in_header = False
                    continue
                if in_header:
                    continue
                # RINEX 3: > 2024 04 24 00 00 00.0000000  0  8
                if line.startswith('>'):
                    parts = line[1:].split()
                    if len(parts) >= 3:
                        y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                        if 2000 <= y <= 2100:
                            return y, m, d
                # RINEX 2: epoch line format: yy mm dd hh mm ss.sss...
                elif len(line) > 25 and line[0] == ' ':
                    try:
                        yy = int(line[1:3])
                        mm = int(line[4:6])
                        dd = int(line[7:9])
                        y  = 2000 + yy if yy < 80 else 1900 + yy
                        if 1980 <= y <= 2100 and 1 <= mm <= 12 and 1 <= dd <= 31:
                            return y, mm, dd
                    except ValueError:
                        continue
    except Exception as e:
        logger.warning(f"Cannot parse RINEX date from {obs_file}: {e}")
    return None


# ── Mirror server definitions ─────────────────────────────────────────────────

MIRRORS_BCAST = [
    # (host, base_path, protocol)
    # KASI path: /gnss/data/daily/{year}/{doy}/{yy}p/ (confirmed working)
    ('nfs.kasi.re.kr', '/gnss/data/daily/{year}/{doy:03d}/{yy:02d}p/', 'ftp'),
    # BKG fallback (jika tersedia)
    ('igs.bkg.bund.de', '/IGS/data/daily/{year}/{doy:03d}/{yy:02d}p/', 'ftp'),
]

MIRRORS_SP3 = [
    ('nfs.kasi.re.kr',  '/gnss/products/{week:04d}/', 'ftp'),
    ('igs.bkg.bund.de', '/IGS/products/{week:04d}/',  'ftp'),
]

MIRRORS_CLK = [
    ('nfs.kasi.re.kr',  '/gnss/products/{week:04d}/', 'ftp'),
    ('igs.bkg.bund.de', '/IGS/products/{week:04d}/',  'ftp'),
]


# ── File naming ───────────────────────────────────────────────────────────────

def bcast_filenames(year: int, doy: int) -> List[str]:
    """Daftar nama file broadcast ephemeris (urutan prioritas)."""
    yy = year % 100
    return [
        # RINEX 3 mixed nav DLR — confirmed ada di KASI /gnss/data/daily/YYYY/DOY/YYp/
        f'BRDM00DLR_S_{year}{doy:03d}0000_01D_MN.rnx.gz',
        # RINEX 3 IGS combined
        f'BRDC00IGS_R_{year}{doy:03d}0000_01D_MN.rnx.gz',
        # Fallback RINEX 2
        f'brdc{doy:03d}0.{yy:02d}n.Z',
        f'brdc{doy:03d}0.{yy:02d}n.gz',
    ]


def sp3_filenames(year: int, doy: int, week: int, dow: int,
                  product: str = 'final') -> List[str]:
    """Daftar nama file SP3 precise orbit."""
    tag = 'FIN' if product == 'final' else 'RAP' if product == 'rapid' else 'ULT'
    return [
        f'IGS0OPS{tag}_{year}{doy:03d}0000_01D_15M_ORB.SP3.gz',
        f'IGS0OPS{tag}_{year}{doy:03d}0000_01D_15M_ORB.sp3.gz',
        f'igs{week:04d}{dow}.sp3.Z',
        f'igs{week:04d}{dow}.sp3.gz',
    ]


def clk_filenames(year: int, doy: int, week: int, dow: int,
                  product: str = 'final') -> List[str]:
    """Daftar nama file precise clock."""
    tag = 'FIN' if product == 'final' else 'RAP'
    interval = '30S' if product == 'final' else '05M'
    return [
        f'IGS0OPS{tag}_{year}{doy:03d}0000_01D_{interval}_CLK.CLK.gz',
        f'igs{week:04d}{dow}.clk.Z',
        f'igs{week:04d}{dow}.clk.gz',
    ]


# ── FTP download ──────────────────────────────────────────────────────────────

def _ftp_list(host: str, path: str, timeout: int = 5) -> List[str]:
    """List files in FTP directory."""
    try:
        with ftplib.FTP(host, timeout=timeout) as ftp:
            ftp.login('anonymous', 'gnss@example.com')
            ftp.cwd(path)
            return ftp.nlst()
    except Exception:
        return []


def _ftp_download(host: str, remote_path: str, local_path: str,
                  timeout: int = 8) -> bool:
    """Download single file via FTP."""
    try:
        with ftplib.FTP(host, timeout=timeout) as ftp:
            ftp.login('anonymous', 'gnss@example.com')
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {remote_path}', f.write)
        return True
    except Exception as e:
        logger.debug(f"FTP download failed {host}{remote_path}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return False


def _http_download(url: str, local_path: str, timeout: int = 8) -> bool:
    """Download via HTTP/HTTPS."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'GNSS-Processor/2.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            with open(local_path, 'wb') as f:
                shutil.copyfileobj(resp, f)
        return True
    except Exception as e:
        logger.debug(f"HTTP download failed {url}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return False


def _decompress(gz_path: str, out_path: str) -> bool:
    """Decompress .gz file."""
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(out_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return True
    except Exception as e:
        logger.debug(f"Decompress failed {gz_path}: {e}")
        return False


# ── Core download functions ───────────────────────────────────────────────────

def _try_download_file(candidates: List[str], mirrors: List[tuple],
                       out_dir: str, year: int, doy: int,
                       week: int = 0, callback=None) -> Optional[str]:
    """
    Coba download salah satu dari candidates dari mirror servers.
    Return path file yang berhasil di-download, atau None.
    """
    os.makedirs(out_dir, exist_ok=True)

    # Batasi max 2 mirror pertama agar tidak stuck lama
    for host, path_tpl, protocol in mirrors[:2]:
        dir_path = path_tpl.format(
            year=year, doy=doy, yy=year%100, week=week
        )
        for fname in candidates[:3]:  # max 3 filename candidate
            local_gz  = os.path.join(out_dir, fname)
            local_out = os.path.join(out_dir, fname.replace('.gz','').replace('.Z',''))

            # Skip jika sudah ada
            if os.path.exists(local_out) and os.path.getsize(local_out) > 1000:
                if callback: callback(f'Cache: {fname}')
                return local_out

            if callback: callback(f'Trying {host} → {fname}')

            ok = False
            if protocol == 'ftp':
                ok = _ftp_download(host, dir_path + fname, local_gz)
            elif protocol in ('http', 'https'):
                url = f'{protocol}://{host}{dir_path}{fname}'
                ok  = _http_download(url, local_gz)

            if ok:
                # Decompress
                if fname.endswith('.gz') or fname.endswith('.Z'):
                    if _decompress(local_gz, local_out):
                        os.remove(local_gz)
                        if callback: callback(f'OK: {os.path.basename(local_out)}')
                        return local_out
                else:
                    if callback: callback(f'OK: {fname}')
                    return local_gz  # already uncompressed

    return None


# ── Public API ────────────────────────────────────────────────────────────────

@dataclass
class EphemerisResult:
    """Result dari auto-download ephemeris."""
    success:     bool  = False
    nav_file:    Optional[str] = None    # broadcast nav
    sp3_file:    Optional[str] = None    # precise orbit
    clk_file:    Optional[str] = None    # precise clock
    year:        int   = 0
    doy:         int   = 0
    week:        int   = 0
    dow:         int   = 0
    product:     str   = 'broadcast'     # 'broadcast' | 'final' | 'rapid'
    messages:    List[str] = field(default_factory=list)
    errors:      List[str] = field(default_factory=list)


def download_ephemeris(obs_file: str, out_dir: str,
                       ephemeris_type: str = 'broadcast',
                       callback=None,
                       timeout_total: int = 30) -> EphemerisResult:
    """
    Auto-download ephemeris untuk file OBS RINEX.

    Parameters
    ----------
    obs_file       : path ke RINEX OBS file
    out_dir        : direktori untuk simpan file ephemeris
    ephemeris_type : 'broadcast' | 'precise' | 'both'
    callback       : function(msg: str) untuk progress update
    timeout_total  : max total detik untuk semua download (default 30s)

    Returns
    -------
    EphemerisResult
    """
    import threading as _threading
    result = EphemerisResult()
    done_evt = _threading.Event()

    def _run():
        _download_ephemeris_inner(obs_file, out_dir, ephemeris_type,
                                  callback, result)
        done_evt.set()

    t = _threading.Thread(target=_run, daemon=True)
    t.start()
    done_evt.wait(timeout=timeout_total)

    if not done_evt.is_set():
        msg = f'Download timeout ({timeout_total}s) — tidak ada koneksi internet'
        result.errors.append(msg)
        result.success = False
        if callback: callback(f'TIMEOUT: {msg}')

    return result


def _download_ephemeris_inner(obs_file: str, out_dir: str,
                               ephemeris_type: str,
                               callback, result: EphemerisResult):
    """Inner function tanpa timeout wrapper."""

    def log(msg):
        result.messages.append(msg)
        logger.info(msg)
        if callback: callback(msg)

    def err(msg):
        result.errors.append(msg)
        logger.warning(msg)
        if callback: callback(f'WARN: {msg}')

    # 1. Parse tanggal dari OBS file
    date = parse_rinex_date(obs_file)
    if not date:
        err(f'Tidak bisa membaca tanggal dari {os.path.basename(obs_file)}')
        return result

    year, month, day = date
    doy  = doy_from_date(year, month, day)
    week, dow = gps_week_dow(year, month, day)

    result.year = year
    result.doy  = doy
    result.week = week
    result.dow  = dow

    log(f'Tanggal observasi: {year}-{month:02d}-{day:02d} (DOY {doy}, GPS Week {week}/{dow})')

    # Cek apakah precise orbit sudah tersedia (IGS Final: H+12-14 hari)
    obs_date  = datetime(year, month, day)
    today     = datetime.now()
    age_days  = (today - obs_date).days
    has_final = age_days >= 14
    has_rapid = age_days >= 2

    # 2. Download broadcast nav
    if ephemeris_type in ('broadcast', 'both'):
        log('Mendownload broadcast ephemeris...')
        nav_candidates = bcast_filenames(year, doy)
        nav_out = _try_download_file(
            nav_candidates, MIRRORS_BCAST, out_dir,
            year, doy, week, callback=log
        )
        if nav_out:
            result.nav_file = nav_out
            log(f'Broadcast nav: {os.path.basename(nav_out)}')
        else:
            err('Broadcast nav tidak bisa didownload dari semua mirror')

    # 3. Download precise orbit + clock
    if ephemeris_type in ('precise', 'both'):
        if not has_rapid and not has_final:
            err(f'Precise orbit belum tersedia — data terlalu baru ({age_days} hari). '
                f'IGS Rapid tersedia setelah ~2 hari, Final setelah ~14 hari.')
        else:
            product = 'final' if has_final else 'rapid'
            log(f'Mendownload precise orbit ({product})...')

            sp3_candidates = sp3_filenames(year, doy, week, dow, product)
            sp3_out = _try_download_file(
                sp3_candidates, MIRRORS_SP3, out_dir,
                year, doy, week, callback=log
            )
            if sp3_out:
                result.sp3_file = sp3_out
                log(f'SP3 orbit: {os.path.basename(sp3_out)}')
            else:
                err(f'SP3 ({product}) tidak bisa didownload')

            # Clock file
            log(f'Mendownload precise clock ({product})...')
            clk_candidates = clk_filenames(year, doy, week, dow, product)
            clk_out = _try_download_file(
                clk_candidates, MIRRORS_CLK, out_dir,
                year, doy, week, callback=log
            )
            if clk_out:
                result.clk_file = clk_out
                log(f'CLK: {os.path.basename(clk_out)}')
            else:
                err(f'CLK ({product}) tidak bisa didownload')

            result.product = product

    # 4. Tentukan success
    if ephemeris_type == 'broadcast':
        result.success = result.nav_file is not None
    elif ephemeris_type == 'precise':
        result.success = result.sp3_file is not None
    else:  # both
        result.success = (result.nav_file is not None or result.sp3_file is not None)

    if result.success:
        log('Download ephemeris selesai')
    else:
        err('Semua download gagal — cek koneksi internet dari LXC')


def get_cached_ephemeris(year: int, doy: int, cache_dir: str,
                         ephemeris_type: str = 'broadcast') -> EphemerisResult:
    """Cek apakah ephemeris untuk tanggal ini sudah ada di cache."""
    result = EphemerisResult(year=year, doy=doy)
    week, dow = gps_week_dow(year, 1, 1)  # approximate
    yy = year % 100

    # Check broadcast
    for fname in bcast_filenames(year, doy):
        clean = fname.replace('.gz','').replace('.Z','')
        path  = os.path.join(cache_dir, clean)
        if os.path.exists(path) and os.path.getsize(path) > 1000:
            result.nav_file = path
            break

    # Check SP3
    for product in ['final', 'rapid']:
        for fname in sp3_filenames(year, doy, week, dow, product):
            clean = fname.replace('.gz','').replace('.Z','')
            path  = os.path.join(cache_dir, clean)
            if os.path.exists(path) and os.path.getsize(path) > 1000:
                result.sp3_file = path
                result.product  = product
                break
        if result.sp3_file:
            break

    result.success = bool(result.nav_file or result.sp3_file)
    return result
