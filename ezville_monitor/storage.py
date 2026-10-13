"""Metadata-only storage measurement. No log contents, credentials or paths exported."""
import glob
import os
from pathlib import Path
import sqlite3


def disk(path):
    try:
        stat = os.statvfs(path)
        total = stat.f_blocks * stat.f_frsize
        free = stat.f_bfree * stat.f_frsize
        return {'available': True, 'total_bytes': total, 'used_bytes': total - free,
                'free_bytes': free, 'available_bytes': stat.f_bavail * stat.f_frsize,
                'used_percent': round((total - free) * 100 / total, 2) if total else None}
    except OSError:
        return {'available': False}


def files(paths):
    size = allocated = count = 0
    complete = True
    for path in paths:
        try:
            stat = Path(path).stat()
            size += stat.st_size
            allocated += getattr(stat, 'st_blocks', (stat.st_size + 511) // 512) * 512
            count += 1
        except OSError:
            complete = False
    return {'file_count': count, 'file_bytes': size, 'allocated_bytes': allocated,
            'measurement_complete': complete}


def measure(database, options, now, previous=None):
    database = Path(database)
    log = Path(options.get('log_path', '/share/simple_mqtt_ezville_control.log'))
    source = files([p for p in glob.glob(str(log) + '*') if Path(p).is_file()])
    source['active_log_exists'] = log.is_file()
    archive = files([p for p in (database, Path(str(database) + '-wal'), Path(str(database) + '-shm')) if p.is_file()])
    archive['active_database_exists'] = database.is_file()
    try:
        with sqlite3.connect('file:' + str(database.resolve()) + '?mode=ro', uri=True) as db:
            page_size = db.execute('PRAGMA page_size').fetchone()[0]
            pages = db.execute('PRAGMA page_count').fetchone()[0]
            reusable = db.execute('PRAGMA freelist_count').fetchone()[0]
        archive.update(logical_bytes=page_size * pages, reusable_bytes=page_size * reusable,
                       occupied_page_bytes=page_size * (pages - reusable))
    except sqlite3.Error:
        archive['database_measurement_available'] = False
    result = {'measured_at': now, 'source_logs': source, 'monitor_archive': archive,
              'source_filesystem': disk(log.parent), 'monitor_filesystem': disk(database.parent),
              'retention_days': options.get('retention_days', 30),
              'archive_limit_bytes': options.get('max_storage_mb', 256) * 1024 * 1024,
              'limitations': ['Filesystem capacity includes other applications; filesystem values must not be added.',
                             'Retained-size changes include rotation/deletion, not total bytes generated.',
                             'Archive limit is a retention budget, not a filesystem quota.']}
    if previous and previous.get('measured_at', now) < now:
        result['comparison_since'] = previous['measured_at']
        for field in ('source_logs', 'monitor_archive'):
            old = previous.get(field, {})
            if old.get('measurement_complete') and result[field]['measurement_complete']:
                result[field]['retained_change_bytes'] = result[field]['file_bytes'] - old['file_bytes']
    return result


def render(value):
    if not value:
        return ['저장공간: 측정 자료 없음']
    def mib(n):
        return f'{n / (1024 * 1024):.2f} MiB'
    lines = ['저장공간 측정 (보고 생성 시점):']
    for field, name in (('source_logs', '원본 로그'), ('monitor_archive', '모니터 DB·WAL·SHM')):
        info = value[field]
        text = f"- {name}: {mib(info['file_bytes'])}, {info['file_count']}개 파일"
        if 'retained_change_bytes' in info:
            text += f" · 이전 보고 대비 보관량 변화 {mib(info['retained_change_bytes'])}"
        if not info.get('active_log_exists', info.get('active_database_exists', True)):
            text += ' · 활성 파일 없음'
        if not info['measurement_complete']:
            text += ' · 일부 파일 측정 실패'
        lines.append(text)
    for field, name in (('source_filesystem', '로그 파일시스템'), ('monitor_filesystem', '모니터 파일시스템')):
        info = value[field]
        lines.append(f"- {name}: 사용 {mib(info['used_bytes'])} / 전체 {mib(info['total_bytes'])}, 사용 가능 {mib(info['available_bytes'])} ({info['used_percent']}% 사용)"
                     if info['available'] else f'- {name}: 측정 불가')
    lines.append(f"- 모니터 보관 예산 {mib(value['archive_limit_bytes'])} · {value['retention_days']}일")
    lines.append('파일시스템 용량에는 다른 앱도 포함되며 두 경로의 용량은 합산하지 않습니다.')
    return lines
