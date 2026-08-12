#!/usr/bin/env python3
"""
Proxy ATM per Fly.io (self-cookie, nessuna dipendenza da Chrome/Mac)
Il cookie F5/Akamai viene ottenuto automaticamente con una GET alla home.
Endpoint:
  GET /atm/12806            -> tutte le linee della fermata
  GET /atm/16995?lines=86   -> solo le linee elencate (csv)
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from curl_cffi import requests as cf
from urllib.parse import urlparse, parse_qs
import os, json, time, threading, math, base64, re
from datetime import datetime
import websocket  # websocket-client
from pyais import decode as ais_decode
try:
    from zoneinfo import ZoneInfo
    _ROME = ZoneInfo("Europe/Rome")
except Exception:
    _ROME = None

ATM_HOME = "https://giromilano.atm.it/"
# ATM ha dismesso il vecchio POST su /proxy.tpportal/proxy.ashx (ora risponde 403
# Access Denied dal WAF). Il sito usa ora un endpoint REST in GET:
ATM_API  = "https://giromilano.atm.it/proxy.tpportal/api/tpPortal/geodata/pois/stops/"
PORT     = int(os.getenv('PORT', 8888))
SESSION_TTL = 300  # rinnova il cookie ogni 5 minuti

_session = None
_session_ts = 0
_lock = threading.Lock()


def _new_session():
    """Crea una sessione e ottiene il cookie F5 (TS01ac3475) via Set-Cookie."""
    s = cf.Session(impersonate='chrome124')
    s.get(ATM_HOME, timeout=15)
    return s


def get_session(force=False):
    global _session, _session_ts
    with _lock:
        now = time.time()
        if force or _session is None or now - _session_ts > SESSION_TTL:
            _session = _new_session()
            _session_ts = now
        return _session


def _do_get(s, stop_code):
    """GET sull'endpoint REST attuale. Il JSON restituito ha la stessa struttura
    di prima (Lines[] con BookletUrl2 / WaitMessage), quindi il parsing non cambia."""
    return s.get(ATM_API + str(stop_code),
        headers={
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'it-IT,it;q=0.9',
            'Referer': 'https://giromilano.atm.it/',
        },
        timeout=15
    )


def fetch_atm(stop_code):
    # Fino a 4 tentativi: il primo usa il cookie corrente, i successivi
    # forzano una sessione nuova (utile dopo un riavvio o cookie scaduto).
    last_status = None
    for attempt in range(4):
        try:
            s = get_session(force=(attempt > 0))
            r = _do_get(s, stop_code)
            if r.status_code == 200:
                return r.json()
            last_status = r.status_code
            print(f"ATM status {r.status_code} per fermata {stop_code} (tentativo {attempt+1})")
        except Exception as e:
            print(f"Fetch error (tentativo {attempt+1}): {e}")
        time.sleep(0.5)
    print(f"ATM fallito dopo 4 tentativi per fermata {stop_code} (ultimo status {last_status})")
    return None


# ============================ AIS (navi) ==================================
# Consuma il flusso AIS gratuito di aisstream.io via websocket, tiene in cache
# le navi viste e le restituisce filtrate su un settore/raggio da casa.
AIS_KEY  = os.getenv('AIS_KEY', '')   # impostala come env var su Render (NON committarla)
# Chiave condivisa per la stazione AIS locale (RTL-SDR + AIS-catcher) che spinge
# dati via POST /ingest (Basic Auth, password = questa chiave). NON committarla.
INGEST_KEY = os.getenv('INGEST_KEY', '')
HOUSE_LAT = 40.9502211
HOUSE_LON = 9.5645113
SECTOR_MIN = 50.0     # gradi (bearing da casa)
SECTOR_MAX = 150.0
MAX_NM     = 20.0     # raggio massimo in miglia nautiche
SHIP_TTL   = 600      # scarta navi non aggiornate da 10 min

# Bounding box (per l'abbonamento aisstream) che racchiude settore+raggio.
_dlat = MAX_NM / 60.0 + 0.05
_dlon = (MAX_NM / 60.0) / math.cos(math.radians(HOUSE_LAT)) + 0.05
BBOX = [[HOUSE_LAT - _dlat, HOUSE_LON - 0.05], [HOUSE_LAT + _dlat, HOUSE_LON + _dlon]]

_ships = {}                    # mmsi -> {name, sog, cog, dest, lat, lon, ts}
_ships_lock = threading.Lock()
_ais_connected = False         # true mentre il websocket aisstream e' su


def _haversine_nm(lat1, lon1, lat2, lon2):
    R = 3440.065  # raggio terrestre in nm
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _bearing(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def _ais_thread():
    if not AIS_KEY:
        print("AIS: nessuna AIS_KEY impostata (env var) -> navi disattivate")
        return
    sub = {
        "APIKey": AIS_KEY,
        "BoundingBoxes": [[[BBOX[0][0], BBOX[0][1]], [BBOX[1][0], BBOX[1][1]]]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
    global _ais_connected
    while True:
        try:
            ws = websocket.create_connection("wss://stream.aisstream.io/v0/stream", timeout=30)
            ws.send(json.dumps(sub))
            print(f"AIS: connesso, bbox={BBOX}")
            _ais_connected = True
            while True:
                msg = json.loads(ws.recv())
                mt = msg.get("MessageType")
                meta = msg.get("MetaData", {}) or {}
                mmsi = meta.get("MMSI")
                if mmsi is None:
                    continue
                with _ships_lock:
                    e = _ships.setdefault(mmsi, {})
                    e["ts"] = time.time()
                    if mt == "PositionReport":
                        pr = msg["Message"]["PositionReport"]
                        e["lat"] = pr.get("Latitude")
                        e["lon"] = pr.get("Longitude")
                        e["sog"] = pr.get("Sog")
                        e["cog"] = pr.get("Cog")
                        if not e.get("name"):
                            e["name"] = (meta.get("ShipName") or "").strip()
                    elif mt == "ShipStaticData":
                        sd = msg["Message"]["ShipStaticData"]
                        nm = (sd.get("Name") or "").strip()
                        if nm:
                            e["name"] = nm
                        e["dest"] = (sd.get("Destination") or "").strip()
        except Exception as ex:
            _ais_connected = False
            print(f"AIS ws error: {ex}; riconnetto tra 5s")
            time.sleep(5)


def get_ships():
    now = time.time()
    out = []
    with _ships_lock:
        for mmsi in list(_ships.keys()):
            e = _ships[mmsi]
            if now - e.get("ts", 0) > SHIP_TTL:
                _ships.pop(mmsi, None)
                continue
            if e.get("lat") is None or e.get("sog") is None:
                continue
            d = _haversine_nm(HOUSE_LAT, HOUSE_LON, e["lat"], e["lon"])
            if d > MAX_NM:
                continue
            b = _bearing(HOUSE_LAT, HOUSE_LON, e["lat"], e["lon"])
            if not (SECTOR_MIN <= b <= SECTOR_MAX):
                continue
            sog = e.get("sog")
            sog = 0.0 if (sog is None or sog >= 102.3) else round(sog, 1)
            cog = e.get("cog")
            cog = 0 if (cog is None or cog >= 360) else int(cog)
            out.append({
                "name": (e.get("name") or "?"),
                "sog": sog,
                "cog": cog,
                "dest": (e.get("dest") or ""),
                "dist": round(d, 1),
                "_d": d,
            })
    out.sort(key=lambda x: x["_d"])
    for o in out:
        o.pop("_d", None)
    return out


# Riceve i messaggi da una stazione AIS locale (RTL-SDR + AIS-catcher, opzione
# -H in formato "AISCATCHER": {"msgs": [{"nmea": ["!AIVDM,..."]}, ...]}).
# Decodifica ogni messaggio (posizione o dati statici/nome nave) e lo fonde
# nella stessa cache _ships usata dal thread aisstream, cosi' get_ships() non
# deve sapere da dove arrivano i dati.
def ingest_ais_payload(payload):
    count = 0
    for m in payload.get("msgs", []):
        lines = m.get("nmea", [])
        if not lines:
            continue
        try:
            dec = ais_decode(*[l.encode() if isinstance(l, str) else l for l in lines])
        except Exception:
            continue
        mmsi = getattr(dec, "mmsi", None)
        if mmsi is None:
            continue
        with _ships_lock:
            e = _ships.setdefault(mmsi, {})
            e["ts"] = time.time()
            lat = getattr(dec, "lat", None)
            lon = getattr(dec, "lon", None)
            if lat is not None:
                e["lat"] = lat
            if lon is not None:
                e["lon"] = lon
            sog = getattr(dec, "speed", None)
            if sog is not None:
                e["sog"] = sog
            cog = getattr(dec, "course", None)
            if cog is not None:
                e["cog"] = cog
            name = getattr(dec, "shipname", None)
            if name and name.strip():
                e["name"] = name.strip()
            dest = getattr(dec, "destination", None)
            if dest and dest.strip():
                e["dest"] = dest.strip()
        count += 1
    return count
# ==========================================================================


# ============================ AEREI (OpenSky) ==============================
# Interroga OpenSky Network (gratuita, nessuna chiave) per gli aerei entro
# PLANE_MAX_NM da casa e sotto PLANE_MAX_ALT_M, poi arricchisce con marche e
# rotta (origine/destinazione IATA) via adsbdb.com. Cache in memoria per non
# martellare adsbdb ad ogni refresh (marche = per sempre, rotte = 1 giorno).
PLANE_MAX_NM    = 10.0     # raggio, nessun settore (360 gradi)
PLANE_MAX_ALT_M = 3000.0   # quota massima (sotto = incluso)
MAX_PLANES      = 3        # quanti arricchire con marche/rotta (= MAX_PAGES sull'ESP)
PLANE_REG_CACHE = {}    # icao24 -> registration (mai scade: e' fissa per l'aereo)
PLANE_RTE_CACHE = {}    # callsign -> (origin_iata, dest_iata, ts)
PLANE_RTE_TTL   = 86400  # 1 giorno
PLANE_REFRESH   = 20     # secondi tra un aggiornamento e l'altro (OpenSky rate-limit ~10s)

# Cache servita all'ESP: OpenSky+adsbdb sono lenti (10-20s), l'ESP ha timeout
# 10s -> un thread aggiorna in background e l'endpoint /planes ritorna subito.
_planes_cache = []
_planes_lock = threading.Lock()

_pdlat = PLANE_MAX_NM / 60.0 + 0.05
_pdlon = (PLANE_MAX_NM / 60.0) / math.cos(math.radians(HOUSE_LAT)) + 0.05
PLANE_BBOX = (HOUSE_LAT - _pdlat, HOUSE_LAT + _pdlat, HOUSE_LON - _pdlon, HOUSE_LON + _pdlon)


def _plane_registration(icao24):
    if icao24 in PLANE_REG_CACHE:
        return PLANE_REG_CACHE[icao24]
    reg = None
    try:
        r = cf.get(f"https://api.adsbdb.com/v0/aircraft/{icao24}", timeout=6)
        if r.status_code == 200:
            reg = (r.json().get("response") or {}).get("aircraft", {}).get("registration")
    except Exception as ex:
        print(f"[planes] registration lookup fallita per {icao24}: {ex}")
    PLANE_REG_CACHE[icao24] = reg
    return reg


def _plane_route(callsign):
    cached = PLANE_RTE_CACHE.get(callsign)
    if cached and (time.time() - cached[2]) < PLANE_RTE_TTL:
        return cached[0], cached[1]
    origin = dest = None
    try:
        r = cf.get(f"https://api.adsbdb.com/v0/callsign/{callsign}", timeout=6)
        if r.status_code == 200:
            rte = (r.json().get("response") or {}).get("flightroute")
            if rte:
                origin = (rte.get("origin") or {}).get("iata_code")
                dest = (rte.get("destination") or {}).get("iata_code")
    except Exception as ex:
        print(f"[planes] route lookup fallita per {callsign}: {ex}")
    PLANE_RTE_CACHE[callsign] = (origin, dest, time.time())
    return origin, dest


PLANE_MAX_ALT_FT = round(PLANE_MAX_ALT_M * 3.28084)   # 3000 m -> ~9843 ft
_planes_diag = {"status": None, "raw": None, "err": None, "ts": 0, "src": None}


def _fetch_adsb():
    """Prova adsb.lol e poi airplanes.live (stesso formato tar1090): entrambe
    gratuite, prendono direttamente punto+raggio e non bloccano gli IP cloud
    come fa OpenSky. Ritorna (lista_ac, sorgente) o ([], None) su errore."""
    n = int(round(PLANE_MAX_NM))
    sources = [
        ("adsb.lol", f"https://api.adsb.lol/v2/lat/{HOUSE_LAT}/lon/{HOUSE_LON}/dist/{n}"),
        ("airplanes.live", f"https://api.airplanes.live/v2/point/{HOUSE_LAT}/{HOUSE_LON}/{n}"),
    ]
    last_err = None
    for name, url in sources:
        try:
            r = cf.get(url, timeout=10)
            if r.status_code == 200:
                ac = r.json().get("ac") or []
                _planes_diag.update(status=r.status_code, raw=len(ac), err=None,
                                    ts=time.time(), src=name)
                return ac, name
            last_err = f"{name} HTTP {r.status_code}"
        except Exception as ex:
            last_err = f"{name}: {ex}"
    _planes_diag.update(status=None, raw=None, err=last_err, ts=time.time(), src=None)
    print(f"[planes] tutte le sorgenti ADS-B fallite: {last_err}")
    return [], None


def _compute_planes():
    ac, _src = _fetch_adsb()

    # Arricchisce TUTTI gli aerei in raggio (qualsiasi quota) con marche e rotta:
    # servono al log completo. Il display filtra poi per quota.
    inrange = []
    for a in ac:
        callsign = (a.get("flight") or "").strip()
        alt = a.get("alt_baro")
        if not callsign or not isinstance(alt, (int, float)):   # "ground" o mancante -> salta
            continue
        dist = a.get("dst")
        if dist is None:
            lat, lon = a.get("lat"), a.get("lon")
            if lat is None or lon is None:
                continue
            dist = _haversine_nm(HOUSE_LAT, HOUSE_LON, lat, lon)
        if dist > PLANE_MAX_NM:
            continue
        origin, dest = _plane_route(callsign)
        inrange.append({
            "callsign": callsign,
            "reg": (a.get("r") or "").strip(),
            "origin": origin or "",
            "dest": dest or "",
            "alt_ft": int(alt),
            "speed_kt": round(a.get("gs") or 0),
            "vrate_fpm": round(a.get("baro_rate") or 0),
            "dist": round(dist, 1),
        })

    # Log: ogni aereo entro il raggio, a QUALSIASI quota (cosi' si sa cosa e'
    # passato anche se troppo alto per il display).
    _update_planes_log(inrange)

    # Display: solo sotto la quota massima, i piu' vicini per primi.
    disp = sorted((r for r in inrange if r["alt_ft"] <= PLANE_MAX_ALT_FT),
                  key=lambda x: x["dist"])
    return disp[:MAX_PLANES]


def _planes_thread():
    global _planes_cache
    while True:
        try:
            result = _compute_planes()   # aggiorna internamente anche il log
            with _planes_lock:
                _planes_cache = result
        except Exception as ex:
            print(f"[planes] refresh fallito: {ex}")
        time.sleep(PLANE_REFRESH)


def get_planes():
    with _planes_lock:
        return list(_planes_cache)


# --- Log dei voli passati (in memoria; sopravvive finche' il processo e' vivo,
# cioe' tenuto sveglio dal cron keep-warm; si azzera a un redeploy). Un record
# per volo (callsign), con orari e valori salienti. Interrogabile su
# GET /planes/log (JSON) o /planes/log?html=1 (tabella leggibile). -----------
PLANE_LOG_MAX = 800
_planes_log = {}                 # callsign -> record
_planes_log_lock = threading.Lock()


def _update_planes_log(planes):
    now = time.time()
    with _planes_log_lock:
        for p in planes:
            cs = p["callsign"]
            direction = "DEP" if p["origin"] == "OLB" else ("ARR" if p["dest"] == "OLB" else "OVF")
            r = _planes_log.get(cs)
            if r is None:
                _planes_log[cs] = {
                    "callsign": cs, "reg": p["reg"], "origin": p["origin"], "dest": p["dest"],
                    "dir": direction, "first_seen": now, "last_seen": now, "sightings": 1,
                    "min_alt_ft": p["alt_ft"], "max_speed_kt": p["speed_kt"], "closest_nm": p["dist"],
                }
            else:
                r["last_seen"] = now
                r["sightings"] += 1
                if p["reg"] and not r["reg"]:       r["reg"] = p["reg"]
                if p["origin"] and not r["origin"]: r["origin"] = p["origin"]
                if p["dest"] and not r["dest"]:     r["dest"] = p["dest"]
                r["dir"] = direction
                r["min_alt_ft"] = min(r["min_alt_ft"], p["alt_ft"])
                r["max_speed_kt"] = max(r["max_speed_kt"], p["speed_kt"])
                r["closest_nm"] = min(r["closest_nm"], p["dist"])
        if len(_planes_log) > PLANE_LOG_MAX:
            excess = len(_planes_log) - PLANE_LOG_MAX
            for o in sorted(_planes_log.values(), key=lambda x: x["last_seen"])[:excess]:
                _planes_log.pop(o["callsign"], None)


def get_planes_log():
    with _planes_log_lock:
        return sorted(_planes_log.values(), key=lambda x: x["last_seen"], reverse=True)


def _hhmm(epoch):
    t = time.gmtime(epoch)
    return f"{t.tm_hour:02d}:{t.tm_min:02d}z"


def _planes_log_html():
    rows = get_planes_log()
    label = {"DEP": "parte OLB", "ARR": "arriva OLB", "OVF": "sorvolo"}
    trs = []
    for r in rows:
        route = f'{r["origin"] or "?"}-{r["dest"] or "?"}'
        trs.append(
            f'<tr><td>{_hhmm(r["first_seen"])}</td><td>{_hhmm(r["last_seen"])}</td>'
            f'<td>{r["callsign"]}</td><td>{r["reg"] or ""}</td><td>{route}</td>'
            f'<td>{label.get(r["dir"], r["dir"])}</td><td>{r["min_alt_ft"]}ft</td>'
            f'<td>{r["max_speed_kt"]}kn</td><td>{r["closest_nm"]}nm</td></tr>'
        )
    return (
        "<!doctype html><meta charset=utf-8><title>Log voli</title>"
        "<style>body{font-family:system-ui,sans-serif;margin:1.5rem;background:#111;color:#eee}"
        "table{border-collapse:collapse;width:100%;font-size:14px}"
        "th,td{border:1px solid #333;padding:5px 9px;text-align:left}"
        "th{background:#1d1d1d}tr:nth-child(even){background:#181818}h1{font-size:18px}</style>"
        f"<h1>Voli passati ({len(rows)}) &mdash; orari UTC</h1>"
        "<table><tr><th>primo</th><th>ultimo</th><th>callsign</th><th>marche</th>"
        "<th>rotta</th><th>tipo</th><th>quota min</th><th>vel max</th><th>dist min</th></tr>"
        + "".join(trs) + "</table>"
    )
# ==========================================================================


# ============================ METAR (Olbia) =================================
# Ultimo METAR di LIEO (Olbia Costa Smeralda) da aviationweather.gov (NOAA,
# gratuita, nessuna chiave), gia' campi separati cosi' l'ESP non deve fare
# parsing di stringhe METAR grezze. Come per gli aerei, un thread aggiorna in
# background e l'endpoint ritorna subito la cache (l'upstream e' lento e il
# METAR cambia ~ogni 30 min).
METAR_ICAO    = "LIEO"
METAR_REFRESH = 300      # secondi (5 min)
METAR_WRAP    = 16       # caratteri per riga remark (font piccolo sull'ESP)
_metar_cache = None
_metar_lock = threading.Lock()


def _wrap_words(text, width):
    """Spezza una stringa in righe da <= width caratteri, senza tagliare parole."""
    lines, cur = [], ""
    for w in text.split():
        if not cur:
            cur = w
        elif len(cur) + 1 + len(w) <= width:
            cur += " " + w
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def _parse_metar(raw):
    """Estrae vento/temp/qnh/remarks dal METAR grezzo. Ritorna un dict parziale;
    i campi non trovati restano None e verra' usato il fallback dai campi JSON."""
    toks = raw.split()
    out = {"wind": None, "td": None, "qnh": None, "remarks": []}

    for t in toks:
        mo = re.match(r'^(\d{3}|VRB)(\d{2,3})(?:G(\d{2,3}))?KT$', t)
        if mo:
            d, sp, g = mo.group(1), mo.group(2), mo.group(3)
            if d == "000" and sp == "00":
                out["wind"] = "CALM"
            else:
                out["wind"] = f"{d}/{sp}" + (f"G{g}" if g else "")
            break
    for t in toks:
        if re.match(r'^M?\d{2}/M?\d{2}$', t):
            out["td"] = t
            break
    pidx = None
    for i, t in enumerate(toks):
        if re.match(r'^[QA]\d{4}$', t):
            out["qnh"] = t
            pidx = i
    if pidx is not None:
        trailing = toks[pidx + 1:]
        if trailing and trailing[0] == "NOSIG":
            trailing = trailing[1:]
        rem = " ".join(trailing).strip()
        if rem:
            out["remarks"] = _wrap_words(rem, METAR_WRAP)
    return out


def _compute_metar():
    try:
        r = cf.get("https://aviationweather.gov/api/data/metar",
                    params={"ids": METAR_ICAO, "format": "json"}, timeout=8)
        if r.status_code != 200:
            print(f"[metar] status {r.status_code}")
            return None
        data = r.json()
        if not data:
            return None
        m = data[0]
        report_time = m.get("reportTime", "")   # "2026-08-11T20:20:00.000Z"
        hhmm = report_time[11:16].replace(":", "") if len(report_time) >= 16 else "----"

        p = _parse_metar(m.get("rawOb", ""))

        # Fallback dai campi strutturati se il parsing grezzo non li ha trovati.
        wind = p["wind"]
        if wind is None:
            wdir = m.get("wdir") if isinstance(m.get("wdir"), int) else 0
            wspd = m.get("wspd") or 0
            wind = "CALM" if wspd == 0 else f"{wdir:03d}/{wspd:02d}"
        td = p["td"]
        if td is None and m.get("temp") is not None and m.get("dewp") is not None:
            td = f"{round(m['temp'])}/{round(m['dewp'])}"
        qnh = p["qnh"]
        if qnh is None and m.get("altim") is not None:
            qnh = f"Q{round(m['altim'])}"

        return {
            "time": f"{hhmm}z",
            "obs_epoch": m.get("obsTime") or 0,
            "wind": wind or "----",
            "td": td or "--/--",
            "qnh": qnh or "----",
            "cat": m.get("fltCat") or "?",
            "remarks": p["remarks"],
        }
    except Exception as ex:
        print(f"[metar] fetch fallito: {ex}")
        return None


def _metar_thread():
    global _metar_cache
    while True:
        result = _compute_metar()
        if result is not None:      # in caso di errore transitorio tieni l'ultimo buono
            with _metar_lock:
                _metar_cache = result
        time.sleep(METAR_REFRESH)


def get_metar():
    with _metar_lock:
        return _metar_cache
# ==========================================================================


# ============================ TRAGHETTI (Olbia + Golfo Aranci) =============
# Scraping degli orari transiti (schedulati, non AIS): Olbia e Golfo Aranci
# arrivano dallo stesso backend portodiolbia.it, tabelle HTML server-rendered.
# Regole di rimozione: "Sbarco terminato" (arrivo) -> via subito; "Partito"
# (partenza) -> via 1 ora dopo l'orario. Un thread aggiorna in background.
FERRY_SOURCES = [
    ("OLB", "https://www.portodiolbia.it/it/porto/orario-transiti"),
    ("GA",  "https://www.portodiolbia.it/home/golfo_aranci"),
]
FERRY_REFRESH = 180      # secondi (3 min)
_ferries_cache = []
_ferries_lock = threading.Lock()


def _parse_ferry_tables(html, harbor):
    out = []
    for t in re.findall(r'<table.*?</table>', html, re.S):
        heads = " ".join(re.sub(r'<[^>]+>', ' ', h) for h in re.findall(r'<th[^>]*>(.*?)</th>', t, re.S)).upper()
        if 'PARTENZA' in heads:
            direction = 'DEP'
        elif 'ARRIVO' in heads:
            direction = 'ARR'
        else:
            continue   # es. tabella autobus di Golfo Aranci
        for row in re.findall(r'<tr[^>]*>(.*?)</tr>', row_wrap(t), re.S):
            cells = [re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', c)).strip()
                     for c in re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)]
            if len(cells) < 3 or not re.match(r'^\d{1,2}:\d{2}$', cells[2]):
                continue
            status = " ".join(cells[3:]).lower()
            molo = cells[-1] if len(cells) >= 4 else ""      # il molo e' l'ultima colonna
            out.append({
                "ship": cells[0], "port": cells[1], "time": cells[2],
                "dir": direction, "harbor": harbor, "status": status, "molo": molo,
            })
    return out


def row_wrap(t):
    # estrae solo il corpo (tbody) se presente, per non prendere l'header <tr>
    m = re.search(r'<tbody.*?</tbody>', t, re.S)
    return m.group(0) if m else t


def _compute_ferries():
    allf = []
    for harbor, url in FERRY_SOURCES:
        try:
            r = cf.get(url, timeout=12)
            if r.status_code == 200:
                allf += _parse_ferry_tables(r.text, harbor)
            else:
                print(f"[ferries] {harbor} HTTP {r.status_code}")
        except Exception as ex:
            print(f"[ferries] {harbor} fallito: {ex}")

    now = datetime.now(_ROME) if _ROME else datetime.utcnow()
    now_min = now.hour * 60 + now.minute

    def tmin(f):
        h, m = f["time"].split(":")
        return int(h) * 60 + int(m)

    shown = []
    for f in allf:
        tm = tmin(f)
        st = f["status"]
        disembarked = 'sbarco' in st and ('terminat' in st or 'complet' in st)
        departed = 'partito' in st
        if f["dir"] == 'ARR' and disembarked:
            continue
        if f["dir"] == 'DEP' and departed and now_min > tm + 60:
            continue
        if now_min - tm > 120:            # oltre 2h nel passato: scarta comunque
            continue
        shown.append({k: f[k] for k in ("ship", "port", "time", "dir", "harbor", "molo")})
    shown.sort(key=tmin)
    return shown


def _ferries_thread():
    global _ferries_cache
    while True:
        try:
            result = _compute_ferries()
            with _ferries_lock:
                _ferries_cache = result
        except Exception as ex:
            print(f"[ferries] refresh fallito: {ex}")
        time.sleep(FERRY_REFRESH)


def get_ferries():
    with _ferries_lock:
        return list(_ferries_cache)
# ==========================================================================


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.address_string()}] {fmt % args}")

    def do_GET(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip('/').split('/')

        # --- Navi (AIS): stato diagnostico (chiave impostata? websocket connesso?) ---
        if parts[:2] == ['ships', 'status']:
            body = json.dumps({
                "ais_key_set": bool(AIS_KEY),
                "connected": _ais_connected,
                "ships_cached": len(_ships),
            }).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- Navi (AIS) ---
        if parts and parts[0] == 'ships':
            body = json.dumps(get_ships()).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- Diagnostica OpenSky (ultimo status HTTP e n. aerei grezzi) ---
        if parts[:2] == ['planes', 'status']:
            body = json.dumps({
                "source": _planes_diag["src"],
                "last_http_status": _planes_diag["status"],
                "raw_aircraft": _planes_diag["raw"],
                "last_error": _planes_diag["err"],
                "age_sec": round(time.time() - _planes_diag["ts"], 1) if _planes_diag["ts"] else None,
                "in_range_now": len(get_planes()),
                "radius_nm": PLANE_MAX_NM,
            }).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- Log dei voli passati (JSON, o ?html=1 per tabella leggibile) ---
        if parts[:2] == ['planes', 'log']:
            qs = parse_qs(parsed.query)
            if qs.get('html'):
                body = _planes_log_html().encode()
                ctype = 'text/html; charset=utf-8'
            else:
                body = json.dumps(get_planes_log()).encode()
                ctype = 'application/json'
            self.send_response(200)
            self.send_header('Content-Type', ctype)
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- Aerei (OpenSky) ---
        if parts and parts[0] == 'planes':
            body = json.dumps(get_planes()).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- METAR (Olbia) ---
        if parts and parts[0] == 'metar':
            body = json.dumps(get_metar()).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- Traghetti (Olbia + Golfo Aranci) ---
        if parts and parts[0] == 'ferries':
            body = json.dumps(get_ferries()).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)
            return

        if len(parts) < 2 or parts[0] != 'atm':
            self.send_response(404)
            self.end_headers()
            return

        stop_code = parts[1]
        qs = parse_qs(parsed.query)
        filter_lines = None
        if 'lines' in qs:
            filter_lines = {x.strip() for x in qs['lines'][0].split(',') if x.strip()}

        data = fetch_atm(stop_code)
        if not data:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b'{"error":"fetch failed"}')
            return

        lines = []
        for l in data.get('Lines', []):
            code = l.get('BookletUrl2') or l.get('Line', {}).get('LineCode', '?')
            wait = l.get('WaitMessage', '-')
            code = str(code).strip()
            if filter_lines is not None and code not in filter_lines:
                continue
            if code and wait:
                lines.append({'line': code, 'wait': str(wait).strip()})

        body = json.dumps(lines).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path.strip('/') != 'ingest':
            self.send_response(404)
            self.end_headers()
            return

        # Basic Auth: username qualsiasi, password = INGEST_KEY.
        auth_ok = False
        if INGEST_KEY:
            auth = self.headers.get('Authorization', '')
            if auth.startswith('Basic '):
                try:
                    _, _, pwd = base64.b64decode(auth[6:]).decode().partition(':')
                    auth_ok = (pwd == INGEST_KEY)
                except Exception:
                    auth_ok = False
        if not auth_ok:
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="ingest"')
            self.end_headers()
            return

        length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(length) if length else b''
        try:
            payload = json.loads(raw)
        except Exception:
            self.send_response(400)
            self.end_headers()
            return

        count = ingest_ais_payload(payload)
        body = json.dumps({"ingested": count}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    # Pre-scalda la sessione (cookie F5) prima di servire, cosi' la prima
    # richiesta dopo un riavvio non fallisce con 503.
    try:
        get_session(force=True)
        print("Sessione pre-scaldata: cookie ATM pronto")
    except Exception as e:
        print(f"Pre-warm fallito (riprovera' alla prima richiesta): {e}")
    # Avvia i worker in background: navi (AIS), aerei (OpenSky), METAR (Olbia).
    # Aerei e METAR aggiornano una cache cosi' l'ESP (timeout 10s) riceve subito.
    threading.Thread(target=_ais_thread, daemon=True).start()
    threading.Thread(target=_planes_thread, daemon=True).start()
    threading.Thread(target=_metar_thread, daemon=True).start()
    threading.Thread(target=_ferries_thread, daemon=True).start()
    print(f"Proxy avviato su http://0.0.0.0:{PORT}  (/atm | /ships | /planes | /metar | /ferries)")
    HTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
