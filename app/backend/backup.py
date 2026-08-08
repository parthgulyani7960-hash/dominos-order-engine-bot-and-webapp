"""Database backup and restore verification utility.

Usage
-----
Backup before a migration:
    python -m app.backend.backup backup

Verify a backup is restorable:
    python -m app.backend.backup verify path/to/backup.db

List recent backups:
    python -m app.backend.backup list
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from pathlib import Path
from typing import Optional

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = _ROOT / "data"
_BACKUP_DIR = _ROOT / "backups"
_DB_PATH = _DATA_DIR / "pizza.db"
_MANIFEST_PATH = _BACKUP_DIR / "manifest.json"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_manifest() -> list[dict]:
    if _MANIFEST_PATH.exists():
        return json.loads(_MANIFEST_PATH.read_text())
    return []


def _save_manifest(entries: list[dict]) -> None:
    _MANIFEST_PATH.write_text(json.dumps(entries, indent=2, default=str))


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def backup(source: Optional[Path] = None) -> Path:
    """Create a timestamped backup of the SQLite database.

    Uses SQLite's online backup API (safe during active connections).
    Records a SHA-256 checksum in the manifest for verification.

    Returns the path to the backup file.
    """
    source = source or _DB_PATH
    if not source.exists():
        raise FileNotFoundError(f"Database not found: {source}")

    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = _BACKUP_DIR / f"pizza_{ts}.db"

    # SQLite online backup (safe for hot backups)
    src_conn = sqlite3.connect(str(source))
    dst_conn = sqlite3.connect(str(dest))
    try:
        src_conn.backup(dst_conn, pages=256)
    finally:
        dst_conn.close()
        src_conn.close()

    checksum = _sha256(dest)
    size = dest.stat().st_size

    entry = {
        "filename": dest.name,
        "path": str(dest),
        "created_at": ts,
        "source": str(source),
        "size_bytes": size,
        "sha256": checksum,
        "verified": False,
    }

    manifest = _load_manifest()
    manifest.append(entry)
    _save_manifest(manifest)

    logger.info("backup_created", path=str(dest), size_bytes=size, sha256=checksum[:16])
    return dest


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------

def verify(backup_path: Optional[Path] = None) -> bool:
    """Verify that a backup can be opened and queried.

    Steps:
    1. Checksum matches manifest.
    2. SQLite can open the file and run PRAGMA integrity_check.
    3. Critical tables exist (users, orders).

    Returns True on success, False on any failure.
    """
    manifest = _load_manifest()
    if not manifest:
        logger.warning("no_backups_found")
        return False

    if backup_path is None:
        # Verify the latest backup
        latest = max(manifest, key=lambda e: e["created_at"])
        backup_path = Path(latest["path"])
        expected_checksum = latest.get("sha256")
    else:
        backup_path = Path(backup_path)
        entry = next((e for e in manifest if Path(e["path"]) == backup_path), None)
        expected_checksum = entry.get("sha256") if entry else None

    if not backup_path.exists():
        logger.error("backup_file_missing", path=str(backup_path))
        return False

    # Step 1: checksum
    actual_checksum = _sha256(backup_path)
    if expected_checksum and actual_checksum != expected_checksum:
        logger.error(
            "checksum_mismatch",
            expected=expected_checksum[:16],
            actual=actual_checksum[:16],
        )
        return False

    # Step 2 + 3: open and integrity-check
    try:
        conn = sqlite3.connect(str(backup_path))
        cursor = conn.cursor()

        result = cursor.execute("PRAGMA integrity_check").fetchone()
        if result[0] != "ok":
            logger.error("integrity_check_failed", result=result[0])
            conn.close()
            return False

        # Step 3: verify critical tables
        tables = {
            row[0]
            for row in cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        required = {"users", "orders"}
        missing = required - tables
        if missing:
            logger.error("missing_tables", tables=missing)
            conn.close()
            return False

        conn.close()
    except Exception as exc:
        logger.error("backup_verify_error", error=str(exc))
        return False

    # Mark as verified in manifest
    for entry in manifest:
        if Path(entry["path"]) == backup_path:
            entry["verified"] = True
            entry["verified_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _save_manifest(manifest)

    logger.info("backup_verified", path=str(backup_path), sha256=actual_checksum[:16])
    return True


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------

def list_backups() -> list[dict]:
    """Return all manifest entries, newest first."""
    manifest = _load_manifest()
    return sorted(manifest, key=lambda e: e["created_at"], reverse=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Database backup utility")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("backup", help="Create a new backup")

    verify_p = sub.add_parser("verify", help="Verify a backup is restorable")
    verify_p.add_argument("path", nargs="?", help="Path to backup file (defaults to latest)")

    sub.add_parser("list", help="List all backups")

    args = parser.parse_args()

    if args.cmd == "backup":
        path = backup()
        ok = verify(path)
        print(f"Backup: {path}  verified={ok}")
        sys.exit(0 if ok else 1)
    elif args.cmd == "verify":
        ok = verify(Path(args.path) if args.path else None)
        print("Verified:", ok)
        sys.exit(0 if ok else 1)
    elif args.cmd == "list":
        entries = list_backups()
        for e in entries:
            print(f"{e['created_at']}  {e['filename']}  {e['size_bytes']:,}B  verified={e['verified']}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
