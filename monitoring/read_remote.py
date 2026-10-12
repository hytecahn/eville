"""Read a monitor through HTTPS, with no HA cookie, password, OTP or OpenAI key."""
import argparse
import hmac
import json
import os
from pathlib import Path
import secrets
import sys
import time
import urllib.parse
import urllib.request
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ezville_monitor'))
from remote_api import MAX_RESPONSE, PATHS, sign


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def query(origin, key, endpoint, payload=None):
    parsed = urllib.parse.urlsplit(origin)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.path not in ('','/') or parsed.query or parsed.fragment:
        raise ValueError('use an HTTPS origin, without credentials/path/query/fragment')
    path = '/v1/' + endpoint
    if path not in PATHS or len(key) < 32:
        raise ValueError('invalid endpoint or key')
    body = json.dumps(payload or {}, separators=(',', ':')).encode()
    stamp, nonce = str(int(time.time())), secrets.token_hex(16)
    request = urllib.request.Request(origin.rstrip('/') + path, body, headers={
        'Content-Type':'application/json','X-Ezville-Time':stamp,'X-Ezville-Nonce':nonce,
        'X-Ezville-Signature':sign(key,'POST',path,stamp,nonce,body)})
    with urllib.request.build_opener(NoRedirect).open(request,timeout=20) as response:
        data = response.read(MAX_RESPONSE + 1)
        response_time = response.headers.get('X-Ezville-Time','')
        if len(data) > MAX_RESPONSE or not response_time.isdigit() or abs(time.time()-int(response_time)) > 60:
            raise ValueError('oversized or stale response')
        expected = sign(key,'RESPONSE:'+str(response.status),path,response_time,nonce,data)
        if not hmac.compare_digest(expected,response.headers.get('X-Ezville-Signature','')):
            raise ValueError('response authentication failed')
    return json.loads(data)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('endpoint',choices=['health','report','recent','daily'])
    p.add_argument('--origin',required=True)
    p.add_argument('--key-file',type=Path)
    p.add_argument('--check',action='store_true',help='Return freshness exit status without exposing telemetry in CI logs')
    a=p.parse_args()
    key = a.key_file.read_text().strip() if a.key_file else os.environ['EZVILLE_REMOTE_SECRET']
    result=query(a.origin,key,a.endpoint)
    if a.check:
        if a.endpoint != 'health':
            raise ValueError('--check requires health')
        if not result.get('collector_receiving') or not result.get('latest_source_exists'):
            raise SystemExit('Monitor collection is stale or source is unavailable.')
        print('Authenticated monitor collection check passed. Device success is unverified.')
    else:
        print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__ == '__main__':
    main()
