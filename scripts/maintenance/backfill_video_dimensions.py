#!/usr/bin/env python3
"""Write width, height, and bitrate_bps on main from archive/index.csv.

Run after migrations/008_video_dimensions.sql. Without --commit, only prints
counts. A row that already has width set is left unchanged.
"""
import argparse
import csv
import json
import os
import re
import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import psycopg
from psycopg.conninfo import conninfo_to_dict

from video_archive.paths import BASE, cli_path
MAIN_REF = 'wbtsfzozlmurljvglhpn'
INT_MAX = 2_147_483_647


def fail(code):
    print(code, file=sys.stderr)
    raise SystemExit(1)


def load_owner(path):
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        fail('owner_config_missing')
    if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077 or path.parent.stat().st_mode & 0o077:
        fail('owner_config_permissions')
    config = json.loads(path.read_text())
    if config.get('project_ref') != MAIN_REF:
        fail('main_project_required')
    info = conninfo_to_dict(config['database_dsn'])
    if (any(k in info for k in ('hostaddr', 'service', 'options'))
            or any(os.environ.get(k) for k in ('PGHOSTADDR', 'PGSERVICE', 'PGOPTIONS'))):
        fail('database_connection_override_forbidden')
    if info.get('sslmode') not in ('verify-full', 'verify-ca', 'require'):
        fail('database_tls_required')
    host = info.get('host') or ''
    user = info.get('user') or ''
    direct = host == f'db.{MAIN_REF}.supabase.co'
    pooled = (re.fullmatch(r'aws-\d+-[a-z0-9-]+\.pooler\.supabase\.com', host) is not None
              and str(info.get('port', '5432')) == '5432'
              and user.endswith('.' + MAIN_REF))
    if not (direct or pooled) or user.startswith('drama_worker'):
        fail('owner_connection_required')
    return config['database_dsn']


def integer(value):
    if value is None or str(value).strip() == '':
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def positive_int(value):
    number = integer(value)
    return number if number is not None and number > 0 else None


def positive_decimal(value):
    if value is None or str(value).strip() == '':
        return None
    try:
        number = Decimal(str(value).strip())
    except Exception:
        return None
    return number if number > 0 else None


def bitrate(size, duration):
    if size is None or duration is None:
        return None
    bps = (Decimal(size) * 8 / duration).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    if bps <= 0 or bps > INT_MAX:
        return None
    return int(bps)


def catalog(path):
    rows = {}
    conflicts = []
    with path.open(newline='', encoding='utf-8-sig') as handle:
        for record in csv.DictReader(handle):
            source_id = integer(record.get('source_id'))
            message_id = positive_int(record.get('message_id'))
            width = positive_int(record.get('width'))
            height = positive_int(record.get('height'))
            if not source_id or message_id is None or width is None or height is None:
                continue
            size = positive_int(record.get('actual_size')) or positive_int(record.get('expected_size'))
            item = {
                'width': width,
                'height': height,
                'bitrate_bps': bitrate(size, positive_decimal(record.get('duration_seconds'))),
            }
            key = (source_id, message_id)
            previous = rows.get(key)
            if previous and (previous['width'], previous['height']) != (width, height):
                conflicts.append(key)
                continue
            if previous is None or (previous['bitrate_bps'] is None and item['bitrate_bps'] is not None):
                rows[key] = item
    if conflicts:
        fail('catalog_dimension_conflict')
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=BASE / '.private/owner-main-pipeline.json')
    parser.add_argument('--catalog', type=Path, default=BASE / 'archive/index.csv')
    parser.add_argument('--commit', action='store_true', help='write rows; omit to only count')
    args = parser.parse_args()
    args.config, args.catalog = cli_path(args.config), cli_path(args.catalog)
    if not args.catalog.is_file() or args.catalog.is_symlink():
        fail('catalog_missing')
    incoming = catalog(args.catalog)
    dsn = load_owner(args.config)
    summary = {
        'catalog_rows': len(incoming),
        'db_videos': 0,
        'matched': 0,
        'already_filled': 0,
        'missing_dimensions': 0,
        'bitrate_missing': 0,
        'updated': 0,
        'committed': False,
    }
    try:
        with psycopg.connect(dsn, connect_timeout=15) as connection:
            connection.execute("SET statement_timeout = '60s'")
            connection.execute("SET lock_timeout = '5s'")
            allowed = connection.execute(
                "SELECT has_table_privilege(current_user, 'drama_archive.videos', 'UPDATE')"
            ).fetchone()[0]
            if not allowed:
                fail('owner_connection_required')
            columns = connection.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'drama_archive' AND table_name = 'videos'
                  AND column_name IN ('width', 'height', 'bitrate_bps')
            """).fetchall()
            if {row[0] for row in columns} != {'width', 'height', 'bitrate_bps'}:
                fail('dimensions_columns_missing')
            videos = connection.execute(
                "SELECT source_id, message_id, width IS NOT NULL FROM drama_archive.videos"
            ).fetchall()
            summary['db_videos'] = len(videos)
            pending = []
            for source_id, message_id, filled in videos:
                item = incoming.get((int(source_id), int(message_id)))
                if item is None:
                    summary['missing_dimensions'] += 1
                    continue
                summary['matched'] += 1
                if item['bitrate_bps'] is None:
                    summary['bitrate_missing'] += 1
                if filled:
                    summary['already_filled'] += 1
                    continue
                pending.append((source_id, message_id, item['width'], item['height'], item['bitrate_bps']))
            summary['updated'] = len(pending)
            if args.commit and pending:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE drama_archive.videos AS v
                        SET width = i.width, height = i.height, bitrate_bps = i.bitrate_bps
                        FROM unnest(%s::bigint[], %s::bigint[], %s::integer[], %s::integer[], %s::integer[])
                          AS i(source_id, message_id, width, height, bitrate_bps)
                        WHERE v.source_id = i.source_id AND v.message_id = i.message_id
                          AND v.width IS NULL AND v.height IS NULL AND v.bitrate_bps IS NULL
                    """, list(zip(*pending)))
                    summary['updated'] = cursor.rowcount
            if args.commit:
                summary['committed'] = True
    except psycopg.Error:
        fail('database_error')
    print(json.dumps(summary, sort_keys=True))


if __name__ == '__main__':
    main()
