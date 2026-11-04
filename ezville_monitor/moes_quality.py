"""Opt-in NUC-local MOES read benchmark; never sends IR, leave or set commands."""
import json
import math
import os
import re
from pathlib import Path
import sqlite3
import statistics
import threading
import time
import uuid
import urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
TARGETS = {'0x3425b4fffe4ed7f2': 'ir_controller_01', '0xf082c0fffef64df2': 'ir_controller_02'}
ROUTER = '0x70d07efffec85528'
BASE = 'zigbee2mqtt/'


def iso(now=None):
    return datetime.fromtimestamp(time.time() if now is None else now, KST).isoformat()


def slot(now):
    dt = datetime.fromtimestamp(now, KST)
    return dt.replace(hour=dt.hour // 6 * 6, minute=0, second=0, microsecond=0).isoformat()


def initialize(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS moes_runs
        (slot TEXT PRIMARY KEY, started REAL, ended REAL, status TEXT, detail TEXT);
        CREATE TABLE IF NOT EXISTS moes_attempts
        (slot TEXT, ieee TEXT, attempt INTEGER, body TEXT,
         PRIMARY KEY(slot,ieee,attempt));''')
    db.commit()


def summarize(attempts):
    ok = [a['rtt_ms'] for a in attempts if a['status'] == 'success']
    ordered = sorted(ok)
    return {'attempts': len(attempts), 'successes': len(ok),
            'success_pct': 100 * len(ok) / len(attempts) if attempts else None,
            'timeouts': sum(a['status'] == 'timeout' for a in attempts),
            'failures': sum(a['status'] == 'failure' for a in attempts),
            'min_ms': min(ok) if ok else None, 'median_ms': statistics.median(ok) if ok else None,
            'p95_ms': ordered[math.ceil(.95 * len(ok)) - 1] if ok else None,
            'max_ms': max(ok) if ok else None, 'p95_method': 'nearest_rank_success_only'}


def read_request(ieee, nwk, transaction, tsn):
    if ieee not in TARGETS or type(nwk) is not int or not 0 < nwk < 0xfff8:
        raise ValueError('invalid_target')
    return {'action': 'raw', 'transaction': transaction, 'params': {
        'ieee_address': ieee, 'network_address': nwk, 'dst_endpoint': 1,
        'cluster_key': 0, 'timeout': 8000, 'disable_response': False,
        'zcl': {'frame_type': 0, 'direction': 0, 'tsn': tsn,
                'command_key': 'read', 'payload': [{'attrId': 0}]}}}


def validate(response, transaction, nwk, tsn):
    """Validate the entire uint8 zclVersion response, not the coordinator's LQI."""
    if response.get('transaction') != transaction:
        raise ValueError('transaction_mismatch')
    if response.get('status') != 'ok':
        raise ValueError('bridge_error')
    d = response.get('data', {})
    if d.get('address') != nwk or d.get('endpoint') != 1 or d.get('clusterID') != 0:
        raise ValueError('address_or_cluster_mismatch')
    header = d.get('header', {})
    if header.get('commandIdentifier') != 1 or header.get('transactionSequenceNumber') != tsn:
        raise ValueError('zcl_header_mismatch')
    buf = d.get('data', {})
    raw = buf.get('data') if isinstance(buf, dict) and buf.get('type') == 'Buffer' else None
    if not isinstance(raw, list) or len(raw) != 8 or any(type(x) is not int or not 0 <= x <= 255 for x in raw):
        raise ValueError('invalid_zcl_buffer')
    # Global, server-to-client, non-manufacturer-specific frame; one uint8 attr.
    if raw[0] & 0x0f != 0x08 or raw[1:7] != [tsn, 1, 0, 0, 0, 0x20]:
        raise ValueError('attribute_or_status_mismatch')
    return raw[7]


def parent_snapshot(status, now):
    result = {'checkedAt': status.get('checkedAt'), 'fresh': False, 'devices': {}}
    try:
        age = now - datetime.fromisoformat(status['checkedAt'].replace('Z', '+00:00')).timestamp()
        result.update(age_seconds=age, fresh=0 <= age <= 600)
    except (KeyError, TypeError, ValueError):
        pass
    for ieee in TARGETS:
        report = status.get('reports', {}).get(ieee, {})
        stamp = report.get('checkedAt')
        try:
            age = now - datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp()
            fresh = result['fresh'] and 0 <= age <= 600
        except (AttributeError, TypeError, ValueError):
            fresh = False
        neighbors = report.get('routerNeighbors', [])
        child = next((n for n in neighbors if str(n.get('eui64', '')).lower() == ieee
                      and n.get('relationship') == 1 and n.get('deviceType') == 2), None)
        parent = report.get('observedParent') if fresh else None
        lqi = child.get('lqi') if child and fresh and parent == ROUTER else None
        if type(lqi) is not int or not 0 <= lqi <= 255:
            lqi = None
        result['devices'][ieee] = {'parent_ieee': parent, 'mg24_lqi': lqi,
                                    'sample_at': stamp, 'fresh': fresh}
    return result


class Session:
    """One unique, clean subscriber per run; no operations on other clients."""
    def __init__(self, credentials):
        import paho.mqtt.client as mqtt
        self.cv = threading.Condition()
        self.retained = {}
        self.pending = {}
        self.ready = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id='moes-quality-' + uuid.uuid4().hex, clean_session=True)
        self.client.username_pw_set(credentials['username'], credentials['password'])
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = lambda *args: self.ready.set()
        self.client.on_message = self.on_message
        self.credentials = credentials

    def on_connect(self, client, userdata, flags, reason, properties):
        if reason == 0:
            client.subscribe([(BASE + x, 0) for x in ('bridge/response/action', 'bridge/devices',
                'bridge/definitions', 'bridge/info', 'mg24_parent_recovery/status')])

    def on_message(self, client, userdata, msg):
        if len(msg.payload) > 2_000_000:
            return
        try:
            data = json.loads(msg.payload)
        except (ValueError, UnicodeError):
            return
        with self.cv:
            if msg.topic == BASE + 'bridge/response/action':
                if isinstance(data, dict) and data.get('transaction') in self.pending and not msg.retain:
                    self.pending[data['transaction']] = data
            else:
                # bridge/info contains configuration: retain only version in memory.
                if msg.topic == BASE + 'bridge/info':
                    data = {'version': data.get('version')} if isinstance(data, dict) else {}
                self.retained[msg.topic.removeprefix(BASE)] = data
            self.cv.notify_all()

    def __enter__(self):
        try:
            self.client.connect(self.credentials['host'], int(self.credentials['port']), 30)
            self.client.loop_start()
            if not self.ready.wait(10):
                raise TimeoutError('mqtt_subscribe_timeout')
            with self.cv:
                if not self.cv.wait_for(lambda: all(k in self.retained for k in
                      ('bridge/devices', 'bridge/definitions', 'bridge/info')), timeout=10):
                    raise TimeoutError('mqtt_metadata_timeout')
                definitions = self.retained['bridge/definitions']
                if 'raw' not in definitions.get('actions', []):
                    raise ValueError('raw_action_unavailable')
            return self
        except Exception:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *args):
        self.client.disconnect()
        self.client.loop_stop()

    def request(self, ieee, attempt):
        transaction = uuid.uuid4().hex
        tsn = attempt % 256
        with self.cv:
            matches = [d for d in self.retained['bridge/devices'] if d.get('ieee_address') == ieee]
            if len(matches) != 1:
                raise ValueError('target_not_unique')
            nwk = matches[0]['network_address']
            payload = read_request(ieee, nwk, transaction, tsn)
            self.pending[transaction] = None
            record = {'at': iso(), 'ieee': ieee, 'network_address': nwk, 'attempt': attempt,
                      'transaction': transaction, 'status': 'failure', 'value': None}
            start = time.monotonic()
            try:
                pub = self.client.publish(BASE + 'bridge/request/action', json.dumps(payload), qos=0, retain=False)
                if pub.rc != 0:
                    raise ValueError('mqtt_publish_failed')
                # ZCL timeout is 8s; allow 2s for the bridge's error response.
                if not self.cv.wait_for(lambda: self.pending[transaction] is not None, timeout=10):
                    record['status'] = 'timeout'
                    record['error'] = 'response_deadline'
                else:
                    response = self.pending[transaction]
                    if response.get('status') == 'error':
                        err = str(response.get('error', '')).lower()
                        record['status'] = 'timeout' if 'timeout' in err or 'timed out' in err else 'failure'
                        record['error'] = 'bridge_timeout' if record['status'] == 'timeout' else 'bridge_error'
                    else:
                        record['value'] = validate(response, transaction, nwk, tsn)
                        record['status'] = 'success'
                        record['zcl_bytes'] = response['data']['data']['data']
            except ValueError as exc:
                record['error'] = str(exc)
            finally:
                record['rtt_ms'] = round((time.monotonic() - start) * 1000, 3)
                del self.pending[transaction]
            return record


def credentials(token):
    request = urllib.request.Request('http://supervisor/services/mqtt',
                                    headers={'Authorization': 'Bearer ' + token})
    with urllib.request.urlopen(request, timeout=5) as response:
        data = json.loads(response.read(65536))['data']
    if data.get('ssl'):
        raise ValueError('mqtt_tls_configuration_required')
    return {k: data[k] for k in ('host', 'port', 'username', 'password')}


def run(database, run_slot, stop, session_factory):
    with sqlite3.connect(database, timeout=10) as db:
        initialize(db)
        with db:
            inserted = db.execute('INSERT OR IGNORE INTO moes_runs VALUES (?,?,NULL,?,?)',
                                 (run_slot, time.time(), 'running', '{}')).rowcount
        if not inserted:
            return False
        detail = {'kind': 'verification' if run_slot.startswith('verification:') else 'scheduled', 'planned_per_device': 20, 'path': 'NUC -> local MQTT -> Z2M raw ZCL',
                  'mac_aps_retries': 'not_measured', 'events': 'not_collected',
                  'ir_success': 'not_tested', 'limitations': ['Read success does not prove IR actuation.']}
        status = 'complete'
        try:
            with session_factory() as session:
                detail['zigbee2mqtt_version'] = session.retained.get('bridge/info', {}).get('version')
                detail['parent_before'] = parent_snapshot(session.retained.get('mg24_parent_recovery/status', {}), time.time())
                for attempt in range(1, 21):
                    for ieee in TARGETS:
                        if stop.is_set():
                            raise InterruptedError()
                        record = session.request(ieee, attempt)
                        with db:
                            db.execute('INSERT INTO moes_attempts VALUES (?,?,?,?)',
                                       (run_slot, ieee, attempt, json.dumps(record)))
                        stop.wait(2)
                detail['parent_after'] = parent_snapshot(session.retained.get('mg24_parent_recovery/status', {}), time.time())
        except Exception as exc:
            status = 'incomplete'
            # Never persist credentials, broker exception text or arbitrary bridge errors.
            detail['error_type'] = type(exc).__name__
        with db:
            db.execute('UPDATE moes_runs SET ended=?,status=?,detail=? WHERE slot=?',
                       (time.time(), status, json.dumps(detail), run_slot))
        return True


def worker(database, options, stop):
    if options.get('moes_quality_enabled') is not True:
        return
    with sqlite3.connect(database) as db:
        initialize(db)
        with db:
            db.execute("UPDATE moes_runs SET status='interrupted',ended=? WHERE status='running'", (time.time(),))
    # Explicit commissioning request, persisted separately from scheduled slots.
    verification_id = options.get('moes_quality_verification_id', '')
    if isinstance(verification_id, str) and re.fullmatch(r'[A-Za-z0-9_-]{1,48}', verification_id):
        run(database, 'verification:' + verification_id, stop,
            lambda: Session(credentials(os.environ['SUPERVISOR_TOKEN'])))
    while not stop.is_set():
        now = time.time()
        key = slot(now)
        # Boot/restart does not fire an unplanned late test or duplicate an old slot.
        if now - datetime.fromisoformat(key).timestamp() <= 900:
            try:
                run(database, key, stop, lambda: Session(credentials(os.environ['SUPERVISOR_TOKEN'])))
            except Exception as exc:
                print('moes-quality: ' + type(exc).__name__, flush=True)
        else:
            try:
                with sqlite3.connect(database, timeout=10) as db:
                    db.execute('INSERT OR IGNORE INTO moes_runs VALUES (?,?,?,?,?)',
                               (key, now, now, 'missed_window', json.dumps({'reason': 'collector_started_after_15_minute_window'})))
            except sqlite3.Error as exc:
                print('moes-quality: ' + type(exc).__name__, flush=True)
        stop.wait(30)


def snapshot(database, options, raw_slot=None):
    if options.get('moes_quality_enabled') is not True:
        return {'observation': 'disabled'}
    try:
        with sqlite3.connect('file:' + str(database) + '?mode=ro', uri=True) as db:
            rows = db.execute('SELECT slot,started,ended,status,detail FROM moes_runs ORDER BY started DESC LIMIT 28').fetchall()
            if raw_slot is not None:
                rows = db.execute('SELECT slot,started,ended,status,detail FROM moes_runs WHERE slot=?', (raw_slot,)).fetchall()
            runs = []
            pools = {24: [], 168: []}
            for key, started, ended, status, detail in rows:
                attempts = [json.loads(r[0]) for r in db.execute('SELECT body FROM moes_attempts WHERE slot=? ORDER BY attempt,ieee', (key,))]
                for hours in pools:
                    if 0 <= time.time() - started <= hours * 3600:
                        pools[hours].extend(attempts)
                runs.append({'slot': key, 'started_at': iso(started), 'ended_at': iso(ended) if ended else None,
                             'status': status, 'detail': json.loads(detail),
                             'devices': {ieee: summarize([a for a in attempts if a['ieee'] == ieee]) for ieee in TARGETS},
                             **({'attempts': attempts} if not runs else {})})
            return {'observation': 'available' if runs else 'no_measurements', 'runs': runs,
                    'windows': {str(h): {i: summarize([a for a in pool if a['ieee'] == i]) for i in TARGETS} for h, pool in pools.items()},
                    'raw_scope': 'latest run only; all original attempts retained on NUC',
                    'events': 'not_collected', 'mac_aps_retries': 'not_measured'}
    except sqlite3.OperationalError:
        return {'observation': 'unavailable'}
