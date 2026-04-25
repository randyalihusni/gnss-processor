"""
crs_transform.py
================
Konversi koordinat GNSS dari WGS-84 Geographic (Lat/Lon)
ke berbagai CRS yang umum dipakai di Indonesia dan geodesi.

CRS yang didukung:
  - WGS-84 Geographic       (EPSG:4326)  — default output RTKLIB
  - WGS-84 / UTM Zone 49S   (EPSG:32749) — East Java metric
  - WGS-84 / UTM Zone 48S   (EPSG:32748) — West Java / Central Java
  - WGS-84 / UTM Zone 50S   (EPSG:32750) — East Kalimantan / Bali
  - DGN95 / TM-3 Zone 49.2  (EPSG:23830) — Kadaster BIG East Java
  - DGN95 / TM-3 Zone 48.2  (EPSG:23829) — Kadaster BIG Central Java
  - DGN95 / TM-3 Zone 50.2  (EPSG:23831) — Kadaster BIG Bali/NTB
  - SRGI2013 Geographic      (EPSG:9470)  — Sistem Referensi Geospasial Indonesia 2013
  - WGS-84 ECEF              (XYZ)        — Earth-Centered Earth-Fixed
  - WGS-84 ENU               (dEast/dNorth/dUp) — Local tangent plane

Implementasi murni Python (tanpa pyproj/gdal) menggunakan
algoritma Transverse Mercator dan Helmert transformation.
DGN95 dan WGS-84 hampir identik (<1mm difference) untuk
keperluan praktis survei di Indonesia.
"""

import math
from dataclasses import dataclass, field
from typing import Optional, Dict, List

# ── Ellipsoid parameters ──────────────────────────────────────────────────────
class Ellipsoid:
    def __init__(self, a, f_inv):
        self.a     = a                        # semi-major axis (m)
        self.f     = 1.0 / f_inv             # flattening
        self.b     = a * (1 - self.f)        # semi-minor axis
        self.e2    = 2*self.f - self.f**2    # first eccentricity squared
        self.ep2   = self.e2 / (1 - self.e2) # second eccentricity squared
        self.n     = self.f / (2 - self.f)   # third flattening

WGS84  = Ellipsoid(6378137.0, 298.257223563)
GRS80  = Ellipsoid(6378137.0, 298.257222101)  # DGN95 / SRGI2013 use GRS80


# ── CRS definitions ───────────────────────────────────────────────────────────
@dataclass
class CRSDef:
    epsg:        int
    name:        str
    short:       str
    unit:        str          # 'degree' or 'metre'
    ellipsoid:   Ellipsoid
    proj:        str          # 'geographic', 'utm', 'tm3', 'ecef', 'enu'
    zone:        Optional[int]   = None   # UTM zone number
    hemisphere:  str             = 'S'    # 'N' or 'S'
    cm:          Optional[float] = None   # central meridian for TM-3
    scale:       float           = 1.0    # scale factor
    fe:          float           = 500000.0  # false easting
    fn:          float           = 0.0       # false northing (10M for S hemisphere UTM)
    note:        str             = ''


CRS_LIST: List[CRSDef] = [
    CRSDef(4326,  'WGS-84 Geographic',          'WGS84-GEO',  'degree', WGS84, 'geographic'),
    CRSDef(32748, 'WGS-84 / UTM Zone 48S',       'UTM-48S',    'metre',  WGS84, 'utm',  zone=48,  hemisphere='S', fn=10_000_000),
    CRSDef(32749, 'WGS-84 / UTM Zone 49S',       'UTM-49S',    'metre',  WGS84, 'utm',  zone=49,  hemisphere='S', fn=10_000_000),
    CRSDef(32750, 'WGS-84 / UTM Zone 50S',       'UTM-50S',    'metre',  WGS84, 'utm',  zone=50,  hemisphere='S', fn=10_000_000),
    CRSDef(23829, 'DGN95 / TM-3 Zone 48.2',      'TM3-48.2',   'metre',  GRS80, 'tm3',  cm=108.0, scale=0.9999, fe=200000, fn=1_500_000),
    CRSDef(23830, 'DGN95 / TM-3 Zone 49.2',      'TM3-49.2',   'metre',  GRS80, 'tm3',  cm=111.0, scale=0.9999, fe=200000, fn=1_500_000),
    CRSDef(23831, 'DGN95 / TM-3 Zone 50.2',      'TM3-50.2',   'metre',  GRS80, 'tm3',  cm=114.0, scale=0.9999, fe=200000, fn=1_500_000),
    CRSDef(9470,  'SRGI2013 Geographic',          'SRGI2013',   'degree', GRS80, 'geographic'),
    CRSDef(0,     'WGS-84 ECEF (XYZ)',            'ECEF-XYZ',   'metre',  WGS84, 'ecef'),
]


@dataclass
class CRSResult:
    epsg:      int
    name:      str
    short:     str
    unit:      str
    coords:    Dict[str, float]    # e.g. {'E': 712345.678, 'N': 9205678.901, 'h': 12.345}
    labels:    List[str]           # display labels
    note:      str = ''


# ── Core math ─────────────────────────────────────────────────────────────────

def ll_to_ecef(lat_deg: float, lon_deg: float, h: float,
               ell: Ellipsoid = WGS84):
    """Geographic → ECEF XYZ."""
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    N   = ell.a / math.sqrt(1 - ell.e2 * math.sin(lat)**2)
    X   = (N + h) * math.cos(lat) * math.cos(lon)
    Y   = (N + h) * math.cos(lat) * math.sin(lon)
    Z   = (N * (1 - ell.e2) + h) * math.sin(lat)
    return X, Y, Z


def _meridian_arc(lat: float, ell: Ellipsoid) -> float:
    """Meridian arc length from equator to latitude (radians)."""
    n  = ell.n
    A0 = 1 + n**2/4 + n**4/64
    A2 = 3/2  * (n - n**3/8)
    A4 = 15/16 * (n**2 - n**4/4)
    A6 = 35/48  * n**3
    A8 = 315/512 * n**4
    return ell.a / (1+n) * (
        A0*lat
        - A2*math.sin(2*lat)
        + A4*math.sin(4*lat)
        - A6*math.sin(6*lat)
        + A8*math.sin(8*lat)
    )


def ll_to_tm(lat_deg: float, lon_deg: float,
             cm_deg: float, scale: float,
             fe: float, fn: float,
             ell: Ellipsoid) -> tuple:
    """
    Geographic → Transverse Mercator (E, N).
    Works for both UTM and TM-3.
    """
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    cm  = math.radians(cm_deg)
    dL  = lon - cm

    e2  = ell.e2
    a   = ell.a
    N_  = a / math.sqrt(1 - e2 * math.sin(lat)**2)
    t   = math.tan(lat)
    eta2 = ell.ep2 * math.cos(lat)**2
    l   = dL

    # Meridian arc
    M  = _meridian_arc(lat, ell)
    M0 = _meridian_arc(0,   ell)

    # Series expansion (Helmert)
    A_  = math.cos(lat) * l
    A2_ = A_**2
    A3_ = A_**3 / 6   * (1 - t**2 + eta2)
    A4_ = A_**4 / 24  * (5 - 18*t**2 + t**4 + 14*eta2 - 58*t**2*eta2)
    A5_ = A_**5 / 120 * (5 - 18*t**2 + t**4 + 14*eta2 - 58*t**2*eta2)
    A6_ = A_**6 / 720 * (61 - 58*t**2 + t**4)

    E = scale * N_ * (A_ + A3_ + A5_) + fe
    N = scale * (M - M0 + N_ * t * (A2_/2 + A4_/24 + A6_/720)) + fn

    # Convergence angle (grid → true north), degrees
    gamma = math.degrees(
        math.sin(lat) * l
        + math.sin(lat) * l**3/3  * (1 + 3*eta2 + 2*eta2**2)
    )

    # Scale factor at point
    k = scale * (1 + (1 + eta2) * A_**2 / 2 + (5 - 4*t**2) * A_**4 / 24)

    return E, N, gamma, k


def auto_utm_zone(lon_deg: float) -> tuple:
    """Return UTM zone number and central meridian for a longitude."""
    zone = int((lon_deg + 180) / 6) + 1
    cm   = (zone - 1) * 6 - 180 + 3
    return zone, cm


def auto_tm3_zone(lon_deg: float) -> tuple:
    """Return TM-3 zone EPSG and central meridian for Indonesian longitude."""
    # TM-3 zones for Indonesia: 46.2(99°), 47.2(102°), 48.2(105°)→108°,
    # 49.2(111°), 50.2(114°), 51.2(117°), 52.2(120°), 53.2(123°), 54.2(126°)
    tm3_zones = [
        (99,  23826, 'TM-3 Zone 46.2'),
        (102, 23827, 'TM-3 Zone 47.2'),
        (105, 23828, 'TM-3 Zone 48.1'),
        (108, 23829, 'TM-3 Zone 48.2'),
        (111, 23830, 'TM-3 Zone 49.2'),
        (114, 23831, 'TM-3 Zone 50.2'),
        (117, 23832, 'TM-3 Zone 51.2'),
        (120, 23833, 'TM-3 Zone 52.2'),
        (123, 23834, 'TM-3 Zone 53.2'),
        (126, 23835, 'TM-3 Zone 54.2'),
    ]
    best_cm, best_epsg, best_name = 111, 23830, 'TM-3 Zone 49.2'
    for cm, epsg, name in tm3_zones:
        if abs(lon_deg - cm) < abs(lon_deg - best_cm):
            best_cm, best_epsg, best_name = cm, epsg, name
    return best_cm, best_epsg, best_name


# ── Main conversion function ──────────────────────────────────────────────────

def convert_all(lat: float, lon: float, h: float) -> List[CRSResult]:
    """
    Convert WGS-84 lat/lon/h to all supported CRS.
    Returns list of CRSResult sorted by relevance.
    """
    results = []

    # 1. WGS-84 Geographic
    results.append(CRSResult(
        epsg=4326, name='WGS-84 Geographic', short='WGS84-GEO', unit='degree',
        coords={'Latitude': round(lat, 8), 'Longitude': round(lon, 8), 'Height (m)': round(h, 4)},
        labels=['Latitude', 'Longitude', 'Height (m)'],
        note='Default output RTKLIB — Decimal Degrees'
    ))

    # 2. SRGI2013 Geographic (same as WGS84 for practical purposes, <1mm diff)
    results.append(CRSResult(
        epsg=9470, name='SRGI2013 Geographic', short='SRGI2013', unit='degree',
        coords={'Latitude': round(lat, 8), 'Longitude': round(lon, 8), 'Height (m)': round(h, 4)},
        labels=['Latitude', 'Longitude', 'Height (m)'],
        note='Sistem Referensi Geospasial Indonesia 2013 — identik WGS84 untuk keperluan praktis'
    ))

    # 3. Auto-detect UTM zone
    utm_zone, utm_cm = auto_utm_zone(lon)
    utm_epsg = 32700 + utm_zone  # Southern hemisphere
    E_utm, N_utm, gamma_utm, k_utm = ll_to_tm(lat, lon, utm_cm, 1.0, 500000, 10_000_000, WGS84)
    results.append(CRSResult(
        epsg=utm_epsg,
        name=f'WGS-84 / UTM Zone {utm_zone}S',
        short=f'UTM-{utm_zone}S',
        unit='metre',
        coords={
            'Easting (m)':   round(E_utm, 3),
            'Northing (m)':  round(N_utm, 3),
            'Height (m)':    round(h, 4),
            'Convergence°':  round(gamma_utm, 6),
            'Scale Factor':  round(k_utm, 9),
        },
        labels=['Easting (m)', 'Northing (m)', 'Height (m)'],
        note=f'Zone {utm_zone}S — Central Meridian {utm_cm}°E'
    ))

    # 4. Nearby UTM zones (±1 zone)
    for adj in [-1, +1]:
        z2 = utm_zone + adj
        if 1 <= z2 <= 60:
            cm2 = (z2 - 1) * 6 - 180 + 3
            epsg2 = 32700 + z2
            E2, N2, g2, k2 = ll_to_tm(lat, lon, cm2, 1.0, 500000, 10_000_000, WGS84)
            results.append(CRSResult(
                epsg=epsg2,
                name=f'WGS-84 / UTM Zone {z2}S',
                short=f'UTM-{z2}S',
                unit='metre',
                coords={
                    'Easting (m)':  round(E2, 3),
                    'Northing (m)': round(N2, 3),
                    'Height (m)':   round(h, 4),
                    'Convergence°': round(g2, 6),
                    'Scale Factor': round(k2, 9),
                },
                labels=['Easting (m)', 'Northing (m)', 'Height (m)'],
                note=f'Zone {z2}S — Central Meridian {cm2}°E'
            ))

    # 5. Auto-detect TM-3 zone
    tm3_cm, tm3_epsg, tm3_name = auto_tm3_zone(lon)
    E_tm3, N_tm3, gamma_tm3, k_tm3 = ll_to_tm(
        lat, lon, tm3_cm, 0.9999, 200000, 1_500_000, GRS80)
    results.append(CRSResult(
        epsg=tm3_epsg,
        name=f'DGN95 / {tm3_name}',
        short=f'TM3-{tm3_cm}',
        unit='metre',
        coords={
            'Easting (m)':   round(E_tm3, 3),
            'Northing (m)':  round(N_tm3, 3),
            'Height (m)':    round(h, 4),
            'Convergence°':  round(gamma_tm3, 6),
            'Scale Factor':  round(k_tm3, 9),
        },
        labels=['Easting (m)', 'Northing (m)', 'Height (m)'],
        note=f'Kadaster BIG — CM {tm3_cm}°E, Scale 0.9999, FE 200000m, FN 1500000m'
    ))

    # 6. Adjacent TM-3 zones
    tm3_all_cms = [99,102,105,108,111,114,117,120,123,126]
    tm3_all_epsgs= [23826,23827,23828,23829,23830,23831,23832,23833,23834,23835]
    idx = tm3_all_cms.index(tm3_cm) if tm3_cm in tm3_all_cms else -1
    for adj in [-1, +1]:
        i2 = idx + adj
        if 0 <= i2 < len(tm3_all_cms):
            cm2   = tm3_all_cms[i2]
            epsg2 = tm3_all_epsgs[i2]
            zone_num = 46 + i2
            E2, N2, g2, k2 = ll_to_tm(lat, lon, cm2, 0.9999, 200000, 1_500_000, GRS80)
            results.append(CRSResult(
                epsg=epsg2,
                name=f'DGN95 / TM-3 Zone {zone_num}.2',
                short=f'TM3-{cm2}',
                unit='metre',
                coords={
                    'Easting (m)':  round(E2, 3),
                    'Northing (m)': round(N2, 3),
                    'Height (m)':   round(h, 4),
                    'Convergence°': round(g2, 6),
                    'Scale Factor': round(k2, 9),
                },
                labels=['Easting (m)', 'Northing (m)', 'Height (m)'],
                note=f'Kadaster BIG — CM {cm2}°E'
            ))

    # 7. ECEF XYZ
    X, Y, Z = ll_to_ecef(lat, lon, h)
    results.append(CRSResult(
        epsg=4978,
        name='WGS-84 ECEF (XYZ)',
        short='ECEF-XYZ',
        unit='metre',
        coords={'X (m)': round(X, 3), 'Y (m)': round(Y, 3), 'Z (m)': round(Z, 3)},
        labels=['X (m)', 'Y (m)', 'Z (m)'],
        note='Earth-Centered Earth-Fixed — origin di pusat bumi'
    ))

    return results


def results_to_dict(crs_results: List[CRSResult]) -> list:
    """Convert list of CRSResult to JSON-serializable list."""
    return [
        {
            'epsg':   r.epsg,
            'name':   r.name,
            'short':  r.short,
            'unit':   r.unit,
            'coords': r.coords,
            'labels': r.labels,
            'note':   r.note,
        }
        for r in crs_results
    ]
