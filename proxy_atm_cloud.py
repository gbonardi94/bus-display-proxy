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
import os, json, time, threading

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


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.address_string()}] {fmt % args}")

    def do_GET(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip('/').split('/')
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
    print(f"Proxy ATM (self-cookie) avviato su http://0.0.0.0:{PORT}/atm/12806")
    HTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
