"""Bounded, read-only Home Assistant operations collector.

Only fixed local API paths and GET/POST read commands are used. The summary
intentionally contains aggregate counts and status fields, never raw logs or
arbitrary entity/configuration data.
"""
import json
import socket
import re
import threading
import time
import urllib.error
import urllib.request

CORE_BASE = 'http://supervisor/core/api'
SUPERVISOR_BASE = 'http://supervisor/supervisor'
MAX_BODY = 1024 * 1024
LOG_LIMIT = 50
ADDON_SLUGS = ('core_mosquitto', 'core_mqtt', 'core_samba', 'core_ssh', 'core_configurator',
               'a0d7b954_ssh', 'a0d7b954_vscode', 'a0d7b954_nut')
SAFE_CODE = re.compile(r'^[A-Z][A-Z0-9_]{0,63}$')
TOTAL_BUDGET_SECONDS = 8
WORKER_BUDGET_SECONDS = 7
POLL_INTERVAL_SECONDS = 900
CORE_PATHS = {'/states'}
SUPERVISOR_PATHS = {'/core/info', '/addons', '/available_updates', '/backups/info', '/host/info', '/resolution/info'}
ADDON_PATH = re.compile(r'^/addons/(?:' + '|'.join(map(re.escape, ADDON_SLUGS)) + r')/(?:info|logs|stats)$')
_CACHE_LOCK = threading.Lock()
_CACHE = {'summary': None, 'last_attempt_at': None, 'last_success_at': None,
          'last_error': None, 'running': False}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class APIError(Exception):
    def __init__(self, observation):
        self.observation = observation


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
            raw = response.read(MAX_BODY + 1)
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
    except (urllib.error.URLError, OSError, ValueError):
        raise APIError('unavailable') from None


def observe(base, path, token, payload=None):
    try:
        return {'observation': 'available', 'data': request(base, path, token, payload)}
    except APIError as exc:
        return {'observation': exc.observation}


def compact_logs(result):
    """Return count buckets and an allowlisted set of error codes, never text."""
    if result['observation'] != 'available':
        return result
    data = result['data']
    entries = data.get('messages', data.get('logs', [])) if isinstance(data, dict) else data
    if not isinstance(entries, list):
        return {'observation': 'unsupported'}
    entries = entries[-LOG_LIMIT:]
    counts = {'error': 0, 'warning': 0, 'other': 0}
    codes = set()
    for item in entries:
        line = item if isinstance(item, str) else json.dumps(item, ensure_ascii=True)
        upper = line.upper()
        level = 'error' if re.search(r'\b(ERROR|EXCEPTION|CRITICAL)\b', upper) else 'warning' if re.search(r'\bWARNING\b', upper) else 'other'
        counts[level] += 1
        for match in re.findall(r'\b[A-Z][A-Z0-9_]{2,63}\b', upper):
            if SAFE_CODE.fullmatch(match) and match not in {'ERROR', 'WARNING', 'EXCEPTION', 'CRITICAL', 'HOMEASSISTANT'}:
                codes.add(match)
    return {'observation': 'available', 'sampled_entries': len(entries), 'buffer_limit': LOG_LIMIT,
            'counts': counts, 'error_codes': sorted(codes)[:30], 'truncated': len(entries) == LOG_LIMIT}


def compact_addons(addons, token, deadline=None, request_fn=None):
    request_fn = request if request_fn is None else request_fn
    if addons['observation'] != 'available':
        return addons
    data = addons['data']
    items = data.get('addons', []) if isinstance(data, dict) else []
    result = []
    for item in items:
        if deadline is not None and time.monotonic() >= deadline:
            break
        slug = item.get('slug')
        if slug not in ADDON_SLUGS:
            continue
        def probe(path):
            if deadline is not None and time.monotonic() >= deadline:
                return {'observation': 'timeout'}
            try:
                left = 1 if deadline is None else max(.05, min(1, deadline - time.monotonic()))
                return {'observation': 'available', 'data': request_fn(SUPERVISOR_BASE, path, token, timeout=left)}
            except APIError as exc:
                return {'observation': exc.observation}
        info = probe('/addons/' + slug + '/info')
        logs = probe('/addons/' + slug + '/logs')
        stats = probe('/addons/' + slug + '/stats')
        i = info.get('data', {})
        result.append({'slug': slug, 'info': {k: i[k] for k in ('state', 'version', 'version_latest', 'update_available') if k in i}
                       if info['observation'] == 'available' else {'observation': info['observation']},
                       'logs': compact_logs(logs),
                       'stats': ({k: stats['data'][k] for k in ('cpu_percent', 'memory_percent', 'memory_usage', 'memory_limit') if k in stats['data']}
                                 if stats['observation'] == 'available' else {'observation': stats['observation']})})
    return {'observation': 'available', 'addons': result, 'allowlisted_count': len(ADDON_SLUGS)}


def compact_updates(result):
    if result['observation'] != 'available':
        return result
    data = result['data']
    items = data if isinstance(data, list) else data.get('updates', data.get('available_updates', [])) if isinstance(data, dict) else []
    if not isinstance(items, list):
        return {'observation': 'unsupported'}
    safe_items = []
    for item in items[:50]:
        if not isinstance(item, dict):
            continue
        slug = item.get('slug')
        if slug not in ADDON_SLUGS and slug not in {'homeassistant', 'supervisor', 'os'}:
            continue
        safe_items.append({'slug': slug, 'current_version': str(item.get('version', ''))[:40],
                           'latest_version': str(item.get('version_latest', ''))[:40]})
    return {'observation': 'available', 'count': len(items), 'items': safe_items,
            'truncated': len(items) > 50}


def summarize_states(result):
    if result['observation'] != 'available':
        return {'observation': result['observation']}
    entities = result['data']
    if not isinstance(entities, list):
        return {'observation': 'unsupported'}
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
            selected[field] = str(item.get('state', ''))[:40]
        elif entity_id in {'sensor.disk_free', 'sensor.disk_use_percent',
                           'sensor.system_monitor_disk_free', 'sensor.system_monitor_disk_use_percent'}:
            selected[entity_id.rsplit('.', 1)[1]] = str(item.get('state', ''))[:40]
    backup_names = ('next_scheduled_automatic_backup', 'last_successful_automatic_backup',
                    'last_attempted_automatic_backup', 'backup_manager_state')
    backup = {key: selected[key] for key in backup_names if key in selected}
    backup['observation'] = 'available' if backup else 'unsupported'
    backup['last_attempt_outcome'] = {'observation': 'unsupported', 'reason': 'outcome_requires_admin_backup_info'}
    backup['fields_observed'] = sorted(key for key in backup if key not in
                                       {'observation', 'fields_observed', 'last_attempt_outcome'})
    return {'observation': 'available', 'backup': backup,
            'storage': {k: selected[k] for k in ('disk_free', 'disk_use_percent') if k in selected},
            'storage_fields_observed': sorted(k for k in ('disk_free', 'disk_use_percent') if k in selected)}


def collect(token, previous=None, deadline=None, request_fn=None):
    """Collect a privacy-minimized HA operations snapshot; all capability errors remain explicit."""
    request_fn = request if request_fn is None else request_fn
    # Patchable reader keeps version/API fixtures testable while preserving the fixed target list.
    def read(base, path, payload=None):
        if deadline is not None and time.monotonic() >= deadline:
            return {'observation': 'timeout'}
        try:
            left = 1 if deadline is None else max(.05, min(1, deadline - time.monotonic()))
            return {'observation': 'available', 'data': request_fn(base, path, token, payload, timeout=left)}
        except APIError as exc:
            return {'observation': exc.observation}
    # Both are Core WebSocket commands in the installed Core source, not REST
    # endpoints. No supported WebSocket proxy is currently available here.
    core_log = {'observation': 'unsupported', 'reason': 'system_log_list_requires_websocket'}
    repairs = {'observation': 'unsupported', 'reason': 'repairs_list_issues_requires_websocket'}
    # HA 2026.10.4 exposes this only through authenticated Core WebSocket commands.
    # The supervisor/core HTTP proxy is not a WebSocket tunnel; retain the explicit
    # capability status until a supported, least-privilege channel exists.
    notifications = {'observation': 'unsupported', 'reason': 'websocket_core_proxy_unavailable'}
    # These are documented Core endpoints. Supervisor endpoints vary by HA version/role.
    core_info = read(SUPERVISOR_BASE, '/core/info')
    addon_list = read(SUPERVISOR_BASE, '/addons')
    update_list = read(SUPERVISOR_BASE, '/available_updates')
    states = read(CORE_BASE, '/states')
    state_summary = summarize_states(states)
    host = read(SUPERVISOR_BASE, '/host/info')
    resolution = read(SUPERVISOR_BASE, '/resolution/info')
    def list_summary(result, key, count_key):
        if result['observation'] != 'available':
            return result
        data = result['data']
        values = data.get(key, []) if isinstance(data, dict) else data
        if not isinstance(values, list):
            return {'observation': 'unsupported'}
        return {'observation': 'available', count_key: len(values)}
    return {'schema': 1, 'core': {
        'version': core_info.get('data', {}).get('version') if core_info['observation'] == 'available' else None,
        'logs': compact_logs(core_log), 'repairs': list_summary(repairs, 'issues', 'open_count'),
        'persistent_notifications': notifications},
        'addons': compact_addons(addon_list, token, deadline, request_fn),
        'backups': state_summary.get('backup', {'observation': state_summary['observation']}),
        'updates': compact_updates(update_list),
        'storage': {'observation': state_summary['observation'],
                    'disk_sensors': state_summary.get('storage', {}),
                    'fields_observed': state_summary.get('storage_fields_observed', [])},
        'host': {k: host['data'][k] for k in ('supervisor', 'operating_system', 'machine', 'disk_total', 'disk_free') if k in host.get('data', {})}
                if host['observation'] == 'available' else {'observation': host['observation']},
        'resolution': list_summary(resolution, 'unhealthy', 'unhealthy_count'),
        'limitations': ['Collected API state does not prove physical device operation or backup restorability.',
                        'Log data is a bounded latest-50 sample, not complete history.']}


def poll_operations(token, executor=None, now_fn=time.time, interval=POLL_INTERVAL_SECONDS,
                    budget=WORKER_BUDGET_SECONDS):
    """Run a cached worker separately from packet log polling, at most every 15 minutes."""
    now = now_fn()
    with _CACHE_LOCK:
        if _CACHE['running']:
            return dict(_CACHE)
        if _CACHE['last_attempt_at'] is not None and now - _CACHE['last_attempt_at'] < interval:
            return dict(_CACHE)
        _CACHE['running'] = True
        _CACHE['last_attempt_at'] = now
    timed_out = False
    try:
        # A single daemon worker cannot stall the packet collector. The caller only
        # waits up to the budget and stale cached values remain readable meanwhile.
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
        executor = executor or ThreadPoolExecutor(max_workers=1, thread_name_prefix='ha-ops')
        future = executor.submit(collect, token, None, time.monotonic() + budget)
        def finished(done):
            try:
                summary = done.result()
            except Exception:
                with _CACHE_LOCK:
                    if _CACHE.get('last_error') != 'timeout':
                        _CACHE['last_error'] = 'collector_error'
                    _CACHE['running'] = False
                return
            with _CACHE_LOCK:
                _CACHE.update(summary=summary, last_success_at=now_fn(), last_error=None, running=False)
        future.add_done_callback(finished)
        try:
            summary = future.result(timeout=budget)
            with _CACHE_LOCK:
                _CACHE.update(summary=summary, last_success_at=now_fn(), last_error=None, running=False)
        except FutureTimeout:
            future.cancel()
            timed_out = True
            with _CACHE_LOCK:
                _CACHE['last_error'] = 'timeout'
    except Exception as exc:
        with _CACHE_LOCK:
            _CACHE['last_error'] = 'collector_error'
    finally:
        with _CACHE_LOCK:
            if not timed_out:
                _CACHE['running'] = False
            result = dict(_CACHE)
    return result


def cached_status(now=None):
    now = time.time() if now is None else now
    with _CACHE_LOCK:
        value = dict(_CACHE)
    last_success = value.get('last_success_at')
    return {'observation': 'available' if last_success is not None and now - last_success <= 1800 else 'stale',
            'last_attempt_at': value.get('last_attempt_at'), 'last_success_at': last_success,
            'last_error': value.get('last_error'),
            'data_age_seconds': None if last_success is None else max(0, now - last_success),
            'running': bool(value.get('running')), 'summary': value.get('summary')}


def restore_cache(db, node):
    """Recover the last operations cache from persistent batches after restart."""
    try:
        rows = db.execute('SELECT body FROM batches WHERE node=? ORDER BY collected DESC LIMIT 1000', (node,))
        for (body,) in rows:
            try:
                value = json.loads(body).get('operations')
            except (ValueError, TypeError, AttributeError):
                continue
            if not isinstance(value, dict) or value.get('summary') is None:
                continue
            with _CACHE_LOCK:
                _CACHE.update(summary=value['summary'], last_attempt_at=value.get('last_attempt_at'),
                              last_success_at=value.get('last_success_at'), last_error=value.get('last_error'),
                              running=False)
            return True
    except Exception:
        return False
    return False
