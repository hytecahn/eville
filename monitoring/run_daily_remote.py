"""Request one manual HA daily report with a dedicated action key, not the read key."""
import argparse
import hmac
import json
import os
from pathlib import Path
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ezville_monitor'))
from remote_api import ACTION_PATH, MAX_RESPONSE, sign_action


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def run(origin, key):
    parsed = urllib.parse.urlsplit(origin)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in ('', '/') or parsed.query or parsed.fragment or len(key) < 32):
        raise ValueError('use an HTTPS origin and a dedicated 32+ character action key')
    body = b'{}'
    stamp, nonce = str(int(time.time())), secrets.token_hex(16)
    request = urllib.request.Request(origin.rstrip('/') + ACTION_PATH, body, headers={
        'Content-Type': 'application/json', 'X-Ezville-Time': stamp, 'X-Ezville-Nonce': nonce,
        'X-Ezville-Signature': sign_action(key, 'POST', ACTION_PATH, stamp, nonce, body)})
    try:
        response = urllib.request.build_opener(NoRedirect).open(request, timeout=35)
    except urllib.error.HTTPError as error:
        response = error  # Verify error responses as carefully as successful ones.
    with response:
        data = response.read(MAX_RESPONSE + 1)
        response_time = response.headers.get('X-Ezville-Time', '')
        if len(data) > MAX_RESPONSE or not response_time.isdigit() or abs(time.time() - int(response_time)) > 60:
            raise ValueError('oversized or stale response')
        expected = sign_action(key, 'RESPONSE:' + str(response.status), ACTION_PATH,
                               response_time, nonce, data)
        if not hmac.compare_digest(expected, response.headers.get('X-Ezville-Signature', '')):
            raise ValueError('response authentication failed')
        result = json.loads(data)
        return response.status, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--origin', required=True)
    parser.add_argument('--key-file', type=Path)
    args = parser.parse_args()
    key = args.key_file.read_text().strip() if args.key_file else os.environ['EZVILLE_ACTION_SECRET']
    status, result = run(args.origin, key)
    print(json.dumps({'status': status, 'ran': result.get('ran'),
                      'window_day': result.get('window_day'), 'message': result.get('message')}, ensure_ascii=False))
    if status != 200:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
