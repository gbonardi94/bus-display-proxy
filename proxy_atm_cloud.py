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
import os, json, time, threading, math
import websocket  # websocket-client

ATM_HOME = "https://giromilano.atm.it/"
ATM_URL  = "https://giromilano.atm.it/proxy.tpportal/proxy.ashx"
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


def _do_post(s, stop_code):
    return s.post(ATM_URL,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'it-IT,it;q=0.9',
            'Referer': 'https://giromilano.atm.it/',
        },
        data=f'url=tpPortal%2Fgeodata%2Fpois%2Fstops%2F{stop_code}',
        timeout=15
    )


def fetch_atm(stop_code):
    # Fino a 4 tentativi: il primo usa il cookie corrente, i successivi
    # forzano una sessione nuova (utile dopo un riavvio o cookie scaduto).
    last_status = None
    for attempt in range(4):
        try:
            s = get_session(force=(attempt > 0))
            r = _do_post(s, stop_code)
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
    while True:
        try:
            ws = websocket.create_connection("wss://stream.aisstream.io/v0/stream", timeout=30)
            ws.send(json.dumps(sub))
            print(f"AIS: connesso, bbox={BBOX}")
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
# ==========================================================================


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.address_string()}] {fmt % args}")

    def do_GET(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip('/').split('/')

        # --- Navi (AIS) ---
        if parts and parts[0] == 'ships':
            body = json.dumps(get_ships()).encode()
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


if __name__ == '__main__':
    # Pre-scalda la sessione (cookie F5) prima di servire, cosi' la prima
    # richiesta dopo un riavvio non fallisce con 503.
    try:
        get_session(force=True)
        print("Sessione pre-scaldata: cookie ATM pronto")
    except Exception as e:
        print(f"Pre-warm fallito (riprovera' alla prima richiesta): {e}")
    # Avvia il consumatore AIS in background (navi).
    threading.Thread(target=_ais_thread, daemon=True).start()
    print(f"Proxy avviato su http://0.0.0.0:{PORT}  (/atm/12806  |  /ships)")
    HTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
