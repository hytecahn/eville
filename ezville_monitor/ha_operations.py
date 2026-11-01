"""Bounded, read-only Home Assistant operations collector.

Only fixed local API paths and GET/POST read commands are used. The summary
intentionally contains aggregate counts and status fields, never raw logs or
arbitrary entity/configuration data.
"""
import base64
import hashlib
import json
import math
import socket
import re
import sqlite3
import secrets
import struct
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime

CORE_BASE = 'http://supervisor/core/api'
SUPERVISOR_BASE = 'http://supervisor'
MAX_BODY = 1024 * 1024
LOG_LIMIT = 50
# Only add-ons identified from this installation's configuration are queried.
# The monitor slug is fixed by the installed add-on; the controller slug comes
# from the existing target_slug option. Never enumerate or guess other slugs.
ADDON_SLUGS = ()
SAFE_CODE = re.compile(r'^[A-Z][A-Z0-9_]{0,63}$')
TOTAL_BUDGET_SECONDS = 8
WORKER_BUDGET_SECONDS = 7
POLL_INTERVAL_SECONDS = 900
CORE_PATHS = {'/states'}
SUPERVISOR_PATHS = {'/core/info', '/supervisor/info', '/os/info', '/host/info', '/resolution/info'}
ADDON_PATH = re.compile(r'^/addons/[a-z0-9_]{1,64}/info$')
MONITOR_ADDON_SLUG = '8a83e751_ezville_monitor'
_CACHE_LOCK = threading.Lock()
_CACHE = {'summary': None, 'last_attempt_at': None, 'last_success_at': None,
          'last_error': None, 'running': False, 'revision': 0}
_WORKER_STARTED_MONOTONIC = None
_WORKER_GENERATION = 0
_WORKER_STUCK_BLOCKED = False
WS_READ_COMMANDS = {'system_log/list', 'repairs/list_issues',
                    'persistent_notification/get', 'backup/info'}
SAFE_ADDON_STATES = {'started', 'stopped', 'unknown', 'error', 'installing',
                     'uninstalling', 'starting', 'stopping', 'updating'}
VERSION_PATTERN = re.compile(r'^(?:\d{1,4}\.){1,3}\d{1,4}(?:[-+][A-Za-z0-9][A-Za-z0-9.-]{0,19})?$')
RFC3339_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$')
DISK_UNIT_BYTES = {'B': 1, 'byte': 1, 'bytes': 1, 'kB': 1000, 'MB': 1000000,
                   'GB': 1000000000, 'TB': 1000000000000, 'KiB': 1024,
                   'MiB': 1048576, 'GiB': 1073741824, 'TiB': 1099511627776}


def safe_version(value):
    return value if isinstance(value, str) and len(value) <= 40 and VERSION_PATTERN.fullmatch(value) else None


def safe_timestamp(value):
    if not isinstance(value, str) or len(value) > 35 or not RFC3339_PATTERN.fullmatch(value):
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + '+00:00' if value.endswith('Z') else value)
    except ValueError:
        return None
    return value if parsed.tzinfo is not None else None


def safe_number(value, minimum=0, maximum=2**63 - 1):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < minimum or number > maximum:
        return None
    return number


def disk_measurement(value, unit, percent=False):
    if not isinstance(unit, str):
        return None
    if percent:
        number = safe_number(value, 0, 100)
        return None if number is None or unit not in {'%', 'percent'} else {'percent': number}
    if unit not in DISK_UNIT_BYTES:
        return None
    number = safe_number(value, 0, 2**63 - 1)
    if number is None:
        return None
    byte_count = number * DISK_UNIT_BYTES[unit]
    if not math.isfinite(byte_count) or byte_count > 2**63 - 1:
        return None
    return {'bytes': int(byte_count)}


def operations_options(options):
    """Fail closed: only literal booleans opt into collection and remote export."""
    if not isinstance(options, dict):
        return {'collection': False, 'remote_export': False}
    return {'collection': options.get('operations_enabled') is True,
            'remote_export': (options.get('operations_enabled') is True
                              and options.get('operations_remote_enabled') is True
                              and options.get('remote_enabled') is True)}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class APIError(Exception):
    def __init__(self, observation):
        self.observation = observation


def _read_response(response, limit, timeout):
    """Read bounded chunks under one absolute deadline when urllib exposes its socket."""
    deadline = time.monotonic() + timeout
    sock = getattr(getattr(getattr(response, 'fp', None), 'raw', None), '_sock', None)
    read_chunk = getattr(response, 'read1', None)
    if sock is None or read_chunk is None:
        raw = response.read(limit + 1)
        if len(raw) > limit:
            raise APIError('unavailable')
        return raw
    raw = bytearray()
    while len(raw) <= limit:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise APIError('timeout')
        sock.settimeout(remaining)
        part = read_chunk(min(8192, limit + 1 - len(raw)))
        if not part:
            return bytes(raw)
        raw.extend(part)
    raise APIError('unavailable')


def request(base, path, token, payload=None, timeout=5):
    """Fetch a fixed path. Never follow redirects or accept caller-supplied URLs."""
    allowed_path = (base == CORE_BASE and path in CORE_PATHS) or (
        base == SUPERVISOR_BASE and (path in SUPERVISOR_PATHS or ADDON_PATH.fullmatch(path)))
    if not allowed_path:
        raise ValueError('invalid fixed API target')
    if payload is not None:
        raise ValueError('only GET is permitted')
    req = urllib.request.Request(base + path, method='GET',
                                 headers={'Authorization': 'Bearer ' + token,
                                          'Content-Type': 'application/json'})
    try:
        opener = urllib.request.build_opener(NoRedirect)
        with opener.open(req, timeout=timeout) as response:
            if response.geturl() != base + path:
                raise APIError('unavailable')
            raw = _read_response(response, MAX_BODY, timeout)
            if len(raw) > MAX_BODY:
                raise APIError('unavailable')
            parsed = json.loads(raw) if raw else {}
            if base == SUPERVISOR_BASE:
                if not isinstance(parsed, dict) or parsed.get('result') != 'ok':
                    raise APIError('unavailable')
                return parsed.get('data', {})
            return parsed
    except urllib.error.HTTPError as exc:
        raise APIError('permission_denied' if exc.code in (401, 403) else 'unsupported' if exc.code == 404 else 'unavailable') from None
    except (TimeoutError, socket.timeout):
        raise APIError('timeout') from None
    except urllib.error.URLError as exc:
        raise APIError('timeout' if isinstance(exc.reason, (TimeoutError, socket.timeout)) else 'unavailable') from None
    except (OSError, ValueError):
        raise APIError('unavailable') from None


def observe(base, path, token, payload=None):
    try:
        return {'observation': 'available', 'attempted': True, 'data': request(base, path, token, payload)}
    except APIError as exc:
        return {'observation': exc.observation, 'attempted': True}


def _ws_send_frame(sock, payload, opcode=1):
    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError('websocket payload must be bytes')
    payload = bytes(payload)
    mask = secrets.token_bytes(4)
    size = len(payload)
    if size < 126:
        header = bytes((0x80 | opcode, 0x80 | size))
    elif size <= 65535:
        header = bytes((0x80 | opcode, 0x80 | 126)) + struct.pack('!H', size)
    else:
        header = bytes((0x80 | opcode, 0x80 | 127)) + struct.pack('!Q', size)
    sock.sendall(header + mask + bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload)))


def _ws_send(sock, value):
    _ws_send_frame(sock, json.dumps(value, separators=(',', ':')).encode(), opcode=1)


def _recv_exact(sock, size, deadline=None):
    data = bytearray()
    while len(data) < size:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise APIError('timeout')
            sock.settimeout(max(.01, remaining))
        part = sock.recv(size - len(data))
        if not part:
            raise APIError('unavailable')
        data.extend(part)
    return bytes(data)


def _ws_read_frame(sock, deadline=None):
    first = _recv_exact(sock, 2, deadline)
    fin, opcode = bool(first[0] & 0x80), first[0] & 0x0f
    if first[0] & 0x70 or opcode not in (0, 1, 2, 8, 9, 10):
        raise APIError('unsupported')
    length = first[1] & 0x7f
    if length == 126:
        length = struct.unpack('!H', _recv_exact(sock, 2, deadline))[0]
    elif length == 127:
        length = struct.unpack('!Q', _recv_exact(sock, 8, deadline))[0]
    if length > MAX_BODY or (opcode >= 8 and (not fin or length > 125)):
        raise APIError('unavailable')
    mask = _recv_exact(sock, 4, deadline) if first[1] & 0x80 else None
    payload = bytearray(_recv_exact(sock, length, deadline))
    if mask:
        payload = bytearray(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return fin, opcode, bytes(payload)


def _ws_decode_message(payload):
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, ValueError):
        raise APIError('unavailable') from None


def websocket_read(token, command, timeout=5, socket_factory=socket.create_connection):
    """Read one allowlisted Core command through the Supervisor add-on API proxy."""
    if command not in WS_READ_COMMANDS:
        raise ValueError('unsupported websocket command')
    deadline = time.monotonic() + timeout
    try:
        sock = socket_factory(('supervisor', 80), timeout=timeout)
    except (OSError, TimeoutError):
        raise APIError('timeout') from None
    try:
        key = base64.b64encode(secrets.token_bytes(16)).decode('ascii')
        request = ('GET /core/websocket HTTP/1.1\r\nHost: supervisor\r\nUpgrade: websocket\r\n'
                   'Connection: Upgrade\r\nSec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\n\r\n'.format(key))
        sock.sendall(request.encode('ascii'))
        response = bytearray()
        while b'\r\n\r\n' not in response and len(response) < 8192:
            response.extend(_recv_exact(sock, 1, deadline))
        header = bytes(response).split(b'\r\n\r\n', 1)[0].decode('latin1')
        accept = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        if ' 101 ' not in header.split('\r\n', 1)[0] or ('sec-websocket-accept: ' + accept).lower() not in header.lower():
            raise APIError('unavailable')
        fragments = None
        while time.monotonic() < deadline:
            fin, opcode, payload = _ws_read_frame(sock, deadline)
            if opcode == 9:
                _ws_send_frame(sock, payload, opcode=10)
                continue
            if opcode == 10:
                continue
            if opcode == 8:
                raise APIError('unavailable')
            if opcode == 1:
                if fragments is not None:
                    raise APIError('unsupported')
                if fin:
                    message = _ws_decode_message(payload)
                else:
                    fragments = bytearray(payload)
                    continue
            elif opcode == 0:
                if fragments is None or len(fragments) + len(payload) > MAX_BODY:
                    raise APIError('unsupported')
                fragments.extend(payload)
                if not fin:
                    continue
                message = _ws_decode_message(bytes(fragments))
                fragments = None
            else:
                raise APIError('unsupported')
            if not isinstance(message, dict):
                raise APIError('unavailable')
            if message.get('type') == 'auth_required':
                _ws_send(sock, {'type': 'auth', 'api_password': token})
            elif message.get('type') == 'auth_invalid':
                raise APIError('permission_denied')
            elif message.get('type') == 'auth_ok':
                _ws_send(sock, {'id': 1, 'type': command})
            elif message.get('type') == 'result' and message.get('id') == 1:
                if not message.get('success'):
                    code = message.get('error', {}).get('code')
                    raise APIError('permission_denied' if code in ('unauthorized', 'auth_invalid') else 'unsupported')
                return message.get('result')
        raise APIError('timeout')
    except (OSError, TimeoutError, IndexError):
        raise APIError('timeout') from None
    finally:
        try:
            sock.close()
        except OSError:
            pass


def observe_ws(token, command, timeout=5):
    try:
        return {'observation': 'available', 'attempted': True, 'data': websocket_read(token, command, timeout=timeout)}
    except APIError as exc:
        return {'observation': exc.observation, 'attempted': True}


def compact_logs(result):
    """Return count buckets and an allowlisted set of error codes, never text."""
    if result['observation'] != 'available':
        return result
    data = result['data']
    entries = data.get('messages', data.get('logs', [])) if isinstance(data, dict) else data
    if isinstance(entries, dict):
        entries = list(entries.values())
    if not isinstance(entries, list):
        return {'observation': 'unsupported', 'attempted': result.get('attempted', True)}
    entries = entries[-LOG_LIMIT:]
    counts = {'error': 0, 'warning': 0, 'other': 0}
    codes = set()
    for item in entries:
        line = item if isinstance(item, str) else json.dumps(item, ensure_ascii=True)
        upper = line.upper()
        level = 'error' if re.search(r'\b(ERROR|EXCEPTION|CRITICAL)\b', upper) else 'warning' if re.search(r'\bWARNING\b', upper) else 'other'
        counts[level] += 1
        # Never derive outbound identifiers from arbitrary log text. Severity
        # counts are the only exported log content until a fixed code registry exists.
    return {'observation': 'available', 'attempted': result.get('attempted', True),
            'sampled_entries': len(entries), 'buffer_limit': LOG_LIMIT,
            'counts': counts, 'error_codes': sorted(codes)[:30], 'truncated': len(entries) == LOG_LIMIT}


def compact_addons(addons, token, slugs=(), deadline=None, request_fn=None):
    request_fn = request if request_fn is None else request_fn
    if addons['observation'] != 'available':
        return addons
    items = [{'slug': slug} for slug in slugs]
    permitted = set(ADDON_SLUGS) | {MONITOR_ADDON_SLUG} | {
        slug for slug in slugs if isinstance(slug, str) and re.fullmatch(r'[a-z0-9_]{1,64}', slug)}
    result = []
    attempted_count = 0
    for item in items:
        slug = item.get('slug')
        if slug not in permitted:
            continue
        def probe(path):
            if deadline is not None and time.monotonic() >= deadline:
                return {'observation': 'timeout', 'attempted': False}
            try:
                left = 1 if deadline is None else max(.05, min(1, deadline - time.monotonic()))
                return {'observation': 'available', 'attempted': True,
                        'data': request_fn(SUPERVISOR_BASE, path, token, timeout=left)}
            except APIError as exc:
                return {'observation': exc.observation, 'attempted': True}
        info = probe('/addons/' + slug + '/info')
        attempted_count += int(info.get('attempted', False))
        i = info.get('data', {})
        if info['observation'] == 'available' and isinstance(i, dict):
            state = i.get('state')
            version = safe_version(i.get('version'))
            latest = safe_version(i.get('version_latest')) if i.get('version_latest') is not None else None
            valid = isinstance(state, str) and state in SAFE_ADDON_STATES and (i.get('version') is None or version is not None) and (
                i.get('version_latest') is None or latest is not None) and (
                i.get('update_available') is None or type(i.get('update_available')) is bool)
            compact_info = {'observation': 'available' if valid else 'unavailable', 'attempted': True}
            if valid:
                compact_info.update({'state': state})
                if version is not None:
                    compact_info['version'] = version
                if latest is not None:
                    compact_info['version_latest'] = latest
                if type(i.get('update_available')) is bool:
                    compact_info['update_available'] = i['update_available']
        else:
            compact_info = {'observation': info['observation'], 'attempted': info.get('attempted', False)}
        result.append({'slug': slug, 'info': compact_info,
                       'logs': {'observation': 'permission_denied', 'attempted': False},
                       'stats': {'observation': 'permission_denied', 'attempted': False}})
    available = sum(item.get('info', {}).get('observation') == 'available' for item in result)
    missing = max(0, len(items) - len(result)) + sum(
        not item.get('info', {}).get('attempted', True) for item in result)
    return {'observation': 'available' if available == len(items) and not missing else 'partial' if available else 'unavailable',
            'attempted': attempted_count > 0, 'attempted_count': attempted_count,
            'allowlisted_count': len(items), 'unattempted_count': missing,
            'coverage': 'monitor_and_configured_controller_only', 'other_addons': 'unmonitored',
            'addons': result}


def compact_updates(result):
    if result['observation'] != 'available':
        return result
    data = result['data']
    items = data if isinstance(data, list) else data.get('updates', data.get('available_updates', [])) if isinstance(data, dict) else []
    if not isinstance(items, list):
        return {'observation': 'unsupported', 'attempted': result.get('attempted', True)}
    safe_items = []
    for item in items[:50]:
        if not isinstance(item, dict):
            continue
        kind = item.get('update_type')
        panel = item.get('panel_path', '')
        if not isinstance(kind, str):
            continue
        if not isinstance(panel, str):
            panel = ''
        if kind in {'core', 'supervisor', 'os'}:
            slug = kind
        else:
            match = re.search(r'/addons/([a-z0-9_]{1,64})(?:/|$)', panel)
            slug = match.group(1) if kind == 'addon' and match else None
        if slug not in set(ADDON_SLUGS) | {MONITOR_ADDON_SLUG, 'core', 'supervisor', 'os'}:
            continue
        latest = safe_version(item.get('version_latest'))
        if latest is not None:
            safe_items.append({'slug': slug, 'latest_version': latest})
    return {'observation': 'available', 'count': len(items), 'items': safe_items,
            'attempted': result.get('attempted', True), 'truncated': len(items) > 50}


def compact_repairs(result):
    if result['observation'] != 'available':
        return result
    data = result['data']
    values = data.get('issues', []) if isinstance(data, dict) else data
    if not isinstance(values, list):
        return {'observation': 'unsupported', 'attempted': result.get('attempted', True)}
    severities = {'critical': 0, 'error': 0, 'warning': 0, 'other': 0}
    for issue in values:
        level = str(issue.get('severity', '')).lower() if isinstance(issue, dict) else ''
        severities[level if level in severities and level != 'other' else 'other'] += 1
    return {'observation': 'available', 'attempted': result.get('attempted', True),
            'open_count': len(values), 'severity_counts': severities}


def compact_notifications(result):
    if result['observation'] != 'available':
        return result
    values = result['data']
    return {'observation': 'available', 'attempted': result.get('attempted', True), 'count': len(values)} if isinstance(values, list) else {'observation': 'unsupported', 'attempted': result.get('attempted', True)}


def compact_backup(result):
    if result['observation'] != 'available':
        return result
    data = result['data']
    if not isinstance(data, dict):
        return {'observation': 'unsupported', 'attempted': result.get('attempted', True)}
    backups = data.get('backups', [])
    if not isinstance(backups, list):
        return {'observation': 'unsupported', 'attempted': result.get('attempted', True)}
    history_partial_failures = 0
    for item in backups:
        if not isinstance(item, dict):
            continue
        history_partial_failures += sum(len(item.get(key, [])) for key in ('failed_addons', 'failed_agent_ids', 'failed_folders')
                                        if isinstance(item.get(key, []), list))
    errors = data.get('agent_errors', {})
    manager_state = data.get('state')
    manager_state_observation = ('available' if isinstance(manager_state, str) and manager_state in {
        'idle', 'create_backup', 'blocked', 'receive_backup', 'restore_backup'} else 'unavailable')
    def timestamp_field(key):
        value = safe_timestamp(data.get(key))
        return {'observation': 'available', 'value': value} if value is not None else {'observation': 'unavailable'}
    return {'observation': 'available', 'attempted': result.get('attempted', True), 'backup_count': len(backups),
            'last_attempted_at': timestamp_field('last_attempted_automatic_backup'),
            'last_successful_at': timestamp_field('last_completed_automatic_backup'),
            'next_scheduled_at': timestamp_field('next_automatic_backup'),
            'manager_state': {'observation': manager_state_observation,
                              **({'value': manager_state} if manager_state_observation == 'available' else {})},
            'agent_error_count': len(errors) if isinstance(errors, dict) else None,
            'history_partial_failure_count': history_partial_failures,
            'latest_attempt_outcome': {'observation': 'unsupported'},
            'limitations': ['An attempt newer than the last completion can be running or failed; those states are not distinguishable here.',
                            'Partial failure count summarizes backup history and does not identify the latest attempt.',
                            'API metadata does not verify backup restoreability.']}


def summarize_states(result):
    if result['observation'] != 'available':
        return {'observation': result['observation'], 'attempted': result.get('attempted', False)}
    entities = result['data']
    if not isinstance(entities, list):
        return {'observation': 'unsupported', 'attempted': result.get('attempted', True)}
    selected = {}
    for item in entities:
        if not isinstance(item, dict):
            continue
        entity_id = item.get('entity_id', '')
        if entity_id.startswith('sensor.backup_') and entity_id in {
            'sensor.backup_next_scheduled_automatic_backup',
            'sensor.backup_last_successful_automatic_backup',
            'sensor.backup_last_attempted_automatic_backup',
            'sensor.backup_backup_manager_state',
        }:
            field = entity_id.removeprefix('sensor.backup_')
            selected[field] = item.get('state')
        elif entity_id in {'sensor.disk_free', 'sensor.system_monitor_disk_free'}:
            attributes = item.get('attributes', {})
            selected['disk_free'] = (item.get('state'), attributes.get('unit_of_measurement')
                                     if isinstance(attributes, dict) else None)
        elif entity_id in {'sensor.disk_use_percent', 'sensor.system_monitor_disk_use_percent'}:
            attributes = item.get('attributes', {})
            selected['disk_use_percent'] = (item.get('state'), attributes.get('unit_of_measurement')
                                            if isinstance(attributes, dict) else None)
    backup_names = ('next_scheduled_automatic_backup', 'last_successful_automatic_backup',
                    'last_attempted_automatic_backup', 'backup_manager_state')
    backup = {}
    for key in backup_names:
        if key not in selected:
            backup[key] = {'observation': 'unsupported'}
        else:
            value = selected[key]
            if key == 'backup_manager_state':
                valid_value = value if isinstance(value, str) and value in {
                    'idle', 'create_backup', 'blocked', 'receive_backup', 'restore_backup'} else None
            else:
                valid_value = safe_timestamp(value)
            backup[key] = {'observation': 'available', 'value': valid_value} if valid_value is not None else {'observation': 'unavailable'}
    if any(v.get('observation') == 'available' for k, v in backup.items() if isinstance(v, dict)):
        backup['observation'] = 'available'
    else:
        backup['observation'] = 'unsupported' if not any(key in selected for key in backup_names) else 'unavailable'
    backup['last_attempt_outcome'] = {'observation': 'unsupported', 'reason': 'outcome_requires_admin_backup_info'}
    backup['fields_observed'] = sorted(key for key in backup if isinstance(backup.get(key), dict)
                                       and backup[key].get('observation') == 'available')
    storage = {}
    for key in ('disk_free', 'disk_use_percent'):
        if key not in selected:
            storage[key] = {'observation': 'unsupported'}
            continue
        raw, unit = selected[key]
        measurement = disk_measurement(raw, unit, percent=(key == 'disk_use_percent'))
        storage[key] = ({'observation': 'available'} | measurement) if measurement is not None else {'observation': 'unavailable'}
    return {'observation': 'available', 'attempted': result.get('attempted', True), 'backup': backup,
            'storage': storage,
            'storage_fields_observed': sorted(k for k, value in storage.items() if value['observation'] == 'available')}


def collect(token, previous=None, deadline=None, request_fn=None, target_slug=''):
    """Collect a privacy-minimized HA operations snapshot; all capability errors remain explicit."""
    request_fn = request if request_fn is None else request_fn
    # Patchable reader keeps version/API fixtures testable while preserving the fixed target list.
    def read(base, path, payload=None):
        if deadline is not None and time.monotonic() >= deadline:
            return {'observation': 'timeout', 'attempted': False}
        try:
            left = 1 if deadline is None else max(.05, min(1, deadline - time.monotonic()))
            return {'observation': 'available', 'attempted': True,
                    'data': request_fn(base, path, token, payload, timeout=left)}
        except APIError as exc:
            return {'observation': exc.observation, 'attempted': True}
    def read_ws(command):
        if deadline is not None and time.monotonic() >= deadline:
            return {'observation': 'timeout', 'attempted': False}
        return observe_ws(token, command, timeout=1 if deadline is None else max(.05, min(1, deadline-time.monotonic())))
    core_log = compact_logs(read_ws('system_log/list'))
    repairs = compact_repairs(read_ws('repairs/list_issues'))
    notifications = compact_notifications(read_ws('persistent_notification/get'))
    backup = compact_backup(read_ws('backup/info'))
    # These are documented Core endpoints. Supervisor endpoints vary by HA version/role.
    core_info = read(SUPERVISOR_BASE, '/core/info')
    supervisor_info = read(SUPERVISOR_BASE, '/supervisor/info')
    os_info = read(SUPERVISOR_BASE, '/os/info')
    states = read(CORE_BASE, '/states')
    state_summary = summarize_states(states)
    host = read(SUPERVISOR_BASE, '/host/info')
    resolution = read(SUPERVISOR_BASE, '/resolution/info')
    addon_slugs = list(dict.fromkeys((MONITOR_ADDON_SLUG,
                                     *((target_slug,) if re.fullmatch(r'[a-z0-9_]{1,64}', target_slug or '') else ()))))
    def list_summary(result, key, count_key):
        if result['observation'] != 'available':
            return result
        data = result['data']
        values = data.get(key, []) if isinstance(data, dict) else data
        if not isinstance(values, list):
            return {'observation': 'unsupported', 'attempted': result.get('attempted', True)}
        return {'observation': 'available', 'attempted': result.get('attempted', True), count_key: len(values)}
    addons = compact_addons({'observation': 'available'}, token, addon_slugs, deadline, request_fn)
    updates = []
    for slug, result in [('homeassistant', core_info), ('supervisor', supervisor_info), ('os', os_info)]:
        value = result.get('data', {})
        if not isinstance(value, dict):
            value = {}
        current = safe_version(value.get('version')) if isinstance(value, dict) else None
        latest = safe_version(value.get('version_latest')) if isinstance(value, dict) else None
        if result.get('observation') == 'available' and value.get('update_available') is True and current and latest:
            updates.append({'slug': slug, 'current_version': current, 'latest_version': latest})
    for addon in addons.get('addons', []):
        info = addon.get('info', {})
        current = safe_version(info.get('version'))
        latest = safe_version(info.get('version_latest'))
        if info.get('update_available') is True and current and latest:
            updates.append({'slug': addon['slug'], 'current_version': current, 'latest_version': latest})
    operations = {'schema': 2, 'core': {
        'version': safe_version(core_info.get('data', {}).get('version')) if core_info['observation'] == 'available' else None,
        'logs': core_log, 'repairs': repairs,
        'persistent_notifications': notifications},
        'addons': addons,
        'backups': backup if backup['observation'] == 'available' else state_summary.get('backup', backup),
        'updates': {'observation': ('available' if all(r.get('observation') == 'available' for r in (core_info, supervisor_info, os_info))
                                    else 'partial' if any(r.get('observation') == 'available' for r in (core_info, supervisor_info, os_info))
                                    else 'unavailable'),
                    'attempted': any(r.get('attempted', False) for r in (core_info, supervisor_info, os_info)),
                    'items': updates, 'count': len(updates)},
        'storage': {'observation': ('available' if any(value.get('observation') == 'available'
                                                       for value in state_summary.get('storage', {}).values()) else
                                    state_summary['observation'] if state_summary['observation'] != 'available' else 'unsupported'),
                    'attempted': state_summary.get('attempted', False),
                    'disk_sensors': state_summary.get('storage', {}),
                    'fields_observed': state_summary.get('storage_fields_observed', [])},
        'host': ({'observation': 'available', 'attempted': host.get('attempted', True)} |
                 {k + '_bytes': int(value) for k in ('disk_total', 'disk_free')
                  if (value := safe_number(host.get('data', {}).get(k))) is not None})
                if host['observation'] == 'available' else {'observation': host['observation'], 'attempted': host.get('attempted', False)},
        'resolution': list_summary(resolution, 'unhealthy', 'unhealthy_count'),
        'limitations': ['Collected API state does not prove physical device operation or backup restorability.',
                        'Only Monitor and the configured target add-on are checked; other add-ons are unmonitored.',
                        'Log, repair and notification counts do not distinguish replacement items when totals stay equal.',
                        'Log data is a bounded latest-50 sample, not complete history.']}
    observations = [operations['core']['logs'], operations['core']['repairs'], operations['core']['persistent_notifications'],
                    operations['backups'], operations['updates'], operations['storage'], operations['host'], operations['resolution']]
    observations.extend(addon.get('info', {}) for addon in addons.get('addons', []))
    good = sum(1 for item in observations if item.get('observation') == 'available' or item.get('state') is not None)
    def section_status(value):
        return {'attempted': bool(value.get('attempted', False)),
                'observation': value.get('observation', 'unavailable')}
    operations['sections'] = {
        'core_logs': section_status(core_log), 'repairs': section_status(repairs),
        'persistent_notifications': section_status(notifications), 'backups': section_status(backup),
        'core_info': section_status(core_info), 'supervisor_info': section_status(supervisor_info),
        'os_info': section_status(os_info), 'core_states': section_status(states),
        'host_info': section_status(host), 'resolution': section_status(operations['resolution']),
        'addons': section_status(addons), 'storage': section_status(operations['storage']),
    }
    operations['observation'] = 'available' if good == len(observations) else 'partial' if good else 'unavailable'
    return operations


def poll_operations(token, executor=None, now_fn=time.time, interval=POLL_INTERVAL_SECONDS,
                    budget=WORKER_BUDGET_SECONDS, target_slug='', stuck_after=None):
    """Submit a cached worker without waiting; packet collection never blocks on it."""
    global _WORKER_STARTED_MONOTONIC, _WORKER_GENERATION, _WORKER_STUCK_BLOCKED
    now = now_fn()
    monotonic_now = time.monotonic()
    with _CACHE_LOCK:
        if _CACHE['running']:
            maximum = budget + 2 if stuck_after is None else stuck_after
            if _WORKER_STARTED_MONOTONIC is None or monotonic_now - _WORKER_STARTED_MONOTONIC <= maximum:
                return dict(_CACHE)
            # Threads cannot be safely killed. Mark the collector stuck and block
            # further submissions in this process instead of queueing unbounded work.
            _WORKER_GENERATION += 1
            _WORKER_STUCK_BLOCKED = True
            _CACHE.update(running=False, last_error='worker_stuck',
                          revision=_CACHE.get('revision', 0) + 1)
        if _WORKER_STUCK_BLOCKED:
            return dict(_CACHE)
        if _CACHE['last_attempt_at'] is not None and now - _CACHE['last_attempt_at'] < interval:
            return dict(_CACHE)
        _CACHE['running'] = True
        _CACHE['last_attempt_at'] = now
        _WORKER_STARTED_MONOTONIC = monotonic_now
        _WORKER_GENERATION += 1
        generation = _WORKER_GENERATION
    def finish(summary=None, failed=False):
        global _WORKER_STARTED_MONOTONIC
        if generation != _WORKER_GENERATION:
            return
        with _CACHE_LOCK:
            if generation != _WORKER_GENERATION:
                return
            if failed:
                _CACHE.update(revision=_CACHE.get('revision', 0) + 1,
                              last_error='collector_error', running=False)
            elif summary.get('observation') != 'unavailable':
                _CACHE['last_success_at'] = now_fn()
                error = 'partial_observation' if summary.get('observation') == 'partial' else None
                _CACHE.update(summary=summary, revision=_CACHE.get('revision', 0) + 1,
                              last_error=error, running=False)
            else:
                _CACHE.update(summary=summary, revision=_CACHE.get('revision', 0) + 1,
                              last_error='all_unavailable', running=False)
            _WORKER_STARTED_MONOTONIC = None
    def run_collect():
        try:
            summary = collect(token, None, time.monotonic() + budget, None, target_slug)
        except Exception:
            finish(failed=True)
        else:
            finish(summary=summary)
    try:
        if executor is None:
            # A stuck DNS/socket call cannot hold add-on shutdown open.
            threading.Thread(target=run_collect, name='ha-operations', daemon=True).start()
        else:
            from concurrent.futures import ThreadPoolExecutor
            future = executor.submit(run_collect)
            future.add_done_callback(lambda done: finish(failed=True) if done.exception() else None)
    except Exception:
        finish(failed=True)
    with _CACHE_LOCK:
        return dict(_CACHE)


def cached_status(now=None):
    now = time.time() if now is None else now
    with _CACHE_LOCK:
        value = dict(_CACHE)
    raw_last_success = value.get('last_success_at')
    last_success = safe_number(raw_last_success, 0, 1e12)
    age = None if last_success is None else now - last_success
    healthy_error = value.get('last_error') in (None, 'partial_observation')
    fresh = age is not None and 0 <= age <= 1800 and healthy_error
    last_error = value.get('last_error')
    last_error = last_error if isinstance(last_error, str) and last_error in {
        'collector_error', 'all_unavailable', 'partial_observation', 'worker_stuck', 'timeout'} else None
    return {'schema': 2, 'observation': 'available' if fresh else 'stale',
            'last_attempt_at': value.get('last_attempt_at'), 'last_success_at': last_success,
            'last_error': last_error, 'revision': value.get('revision', 0),
            'data_age_seconds': None if age is None else max(0, age),
            'running': bool(value.get('running')), 'summary': value.get('summary')}


def restore_cache(db, node):
    """Restore the indexed latest snapshot after restart; no batch scan."""
    global _WORKER_STARTED_MONOTONIC, _WORKER_GENERATION, _WORKER_STUCK_BLOCKED
    try:
        row = db.execute('SELECT body FROM operations_snapshots WHERE node=?', (node,)).fetchone()
        if row is None:
            return False
        value = json.loads(row[0])
        # Schema 1 summaries were produced before strict validation and may
        # contain arbitrary strings. Never restore or re-export them.
        if not isinstance(value, dict) or value.get('schema') != 2:
            with _CACHE_LOCK:
                _CACHE.update(summary=None, last_attempt_at=None, last_success_at=None,
                              last_error=None, running=False)
                _WORKER_STARTED_MONOTONIC = None
                _WORKER_STUCK_BLOCKED = False
                _WORKER_GENERATION += 1
            return False
        with _CACHE_LOCK:
            _CACHE.update(value, running=False)
            _WORKER_STARTED_MONOTONIC = None
            _WORKER_STUCK_BLOCKED = False
            _WORKER_GENERATION += 1
        return True
    except (sqlite3.Error, ValueError, TypeError, AttributeError):
        return False
