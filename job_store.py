"""
job_store.py
============
Persistent job storage menggunakan JSON file.
Job tidak hilang saat container restart atau browser refresh.

Struktur:
  results/
    job_store.json   ← semua single jobs
    batch_store.json ← semua batch jobs
"""

import os
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

STORE_DIR  = 'results'
JOB_FILE   = os.path.join(STORE_DIR, 'job_store.json')
BATCH_FILE = os.path.join(STORE_DIR, 'batch_store.json')

_lock = threading.Lock()


def _ensure_dir():
    os.makedirs(STORE_DIR, exist_ok=True)


# ── Single job store ──────────────────────────────────────────────────────────

def load_jobs() -> Dict[str, Any]:
    _ensure_dir()
    try:
        if os.path.exists(JOB_FILE):
            with open(JOB_FILE, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_jobs(jobs: Dict[str, Any]):
    _ensure_dir()
    with _lock:
        try:
            with open(JOB_FILE, 'w') as f:
                # Exclude large stdout/stderr from persist
                slim = {}
                for jid, j in jobs.items():
                    s = dict(j)
                    s['stdout'] = (s.get('stdout') or '')[:500]
                    s['stderr'] = (s.get('stderr') or '')[:500]
                    slim[jid] = s
                json.dump(slim, f, default=str)
        except Exception as e:
            pass


def save_job(jobs: Dict[str, Any], jid: str):
    """Save single job update."""
    save_jobs(jobs)


# ── Batch store ───────────────────────────────────────────────────────────────

def load_batch_jobs() -> Dict[str, Any]:
    _ensure_dir()
    try:
        if os.path.exists(BATCH_FILE):
            with open(BATCH_FILE, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_batch_jobs(batch_jobs_dict: Dict[str, Any]):
    _ensure_dir()
    with _lock:
        try:
            slim = {}
            for bid, b in batch_jobs_dict.items():
                s = dict(b)
                # Slim down points summary
                if 'points' in s:
                    pts = []
                    for p in s['points']:
                        pt = dict(p)
                        pt['eph_messages'] = pt.get('eph_messages', [])[-3:]
                        pts.append(pt)
                    s['points'] = pts
                slim[bid] = s
            with open(BATCH_FILE, 'w') as f:
                json.dump(slim, f, default=str)
        except Exception:
            pass


# ── Age helpers ───────────────────────────────────────────────────────────────

def is_recent(job: Dict, hours: int = 24) -> bool:
    """Return True jika job dibuat dalam N jam terakhir."""
    try:
        created = job.get('created_at', '') or job.get('created_at', '')
        if not created:
            return True
        dt = datetime.fromisoformat(str(created)[:19])
        return datetime.now() - dt < timedelta(hours=hours)
    except Exception:
        return True


def job_age_str(job: Dict) -> str:
    """Return human-readable age string."""
    try:
        created = str(job.get('created_at', ''))[:19]
        dt = datetime.fromisoformat(created)
        delta = datetime.now() - dt
        s = int(delta.total_seconds())
        if s < 60:    return f'{s}s ago'
        if s < 3600:  return f'{s//60}m ago'
        if s < 86400: return f'{s//3600}h ago'
        return f'{s//86400}d ago'
    except Exception:
        return ''


def categorize_jobs(jobs: Dict) -> tuple:
    """
    Split jobs menjadi active (<=24h) dan history (>24h).
    Return (active_dict, history_dict)
    """
    active  = {}
    history = {}
    for jid, j in jobs.items():
        if is_recent(j, hours=24):
            active[jid]  = j
        else:
            history[jid] = j
    return active, history
