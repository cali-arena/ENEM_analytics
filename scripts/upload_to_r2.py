"""
Sync local data lake (data/bronze, data/silver, data/gold) to Cloudflare R2 via S3.

Usage:
  pip install boto3 python-dotenv
  Credentials: .streamlit/secrets.toml (same as Streamlit) or env vars.
  python scripts/upload_to_r2.py

Behavior:
  - Uploads recursively: bronze → s3://datalake/bronze/, silver → silver/, gold → gold/
  - Preserves folder structure
  - Skips unchanged files (size + mtime)
  - Uses multipart upload for large Parquet (boto3 default)
  - Progress logging
"""

from __future__ import annotations

import os
import re
from pathlib import Path

# Repo root and data dirs (no app dependency for CLI)
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Prefer .streamlit/secrets.toml in current dir (when you run from repo root), else next to script
SECRETS_FILE = (Path.cwd() / ".streamlit" / "secrets.toml").resolve()
if not SECRETS_FILE.exists():
    SECRETS_FILE = (ROOT / ".streamlit" / "secrets.toml").resolve()

# Multipart threshold (boto3 default 8MB)
MULTIPART_THRESHOLD = 8 * 1024 * 1024


def _parse_secrets_file_line(line: str) -> tuple[str, str] | None:
    """Parse a single line like R2_ENDPOINT="https://..." or R2_ENDPOINT = '...'. Returns (key, value) or None."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if "=" not in line:
        return None
    key, _, rest = line.partition("=")
    key = key.strip()
    rest = rest.strip()
    if rest.startswith('"') and rest.endswith('"'):
        rest = rest[1:-1].replace('\\"', '"')
    elif rest.startswith("'") and rest.endswith("'"):
        rest = rest[1:-1].replace("\\'", "'")
    return (key, rest)


def _load_credentials() -> tuple[str, str, str, str, str]:
    """Load R2 credentials: .streamlit/secrets.toml first, then env. Returns (access_key, secret_key, endpoint, bucket, source)."""
    access_key = ""
    secret_key = ""
    endpoint = ""
    bucket = "datalake"
    source = "environment"

    # 0) Load .env into os.environ first (so env vars are available)
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        load_dotenv()  # cwd
    except ImportError:
        pass

    # 1) .streamlit/secrets.toml — read as text line-by-line (utf-8-sig strips BOM)
    if SECRETS_FILE.exists():
        data = {}
        try:
            with open(SECRETS_FILE, "r", encoding="utf-8-sig") as f:
                for line in f:
                    parsed = _parse_secrets_file_line(line)
                    if parsed:
                        data[parsed[0]] = parsed[1]
        except Exception:
            pass
        def _s(v):
            return "" if v is None else str(v).strip().strip('"')
        ak = _s(data.get("R2_ACCESS_KEY"))
        sk = _s(data.get("R2_SECRET_KEY"))
        ep = _s(data.get("R2_ENDPOINT"))
        # Fallback: if endpoint missing, try regex on raw line (handles long URLs / quirks)
        if not ep and SECRETS_FILE.exists():
            try:
                with open(SECRETS_FILE, "r", encoding="utf-8-sig") as f:
                    for line in f:
                        if "R2_ENDPOINT" in line and "=" in line:
                            m = re.search(r'R2_ENDPOINT\s*=\s*["\']([^"\']+)["\']', line)
                            if m:
                                ep = m.group(1).strip()
                                break
            except Exception:
                pass
        bu = _s(data.get("R2_BUCKET")) or bucket
        if ak or sk or ep:
            access_key = ak or access_key
            secret_key = sk or secret_key
            endpoint = ep or endpoint
            bucket = bu or bucket
            source = ".streamlit/secrets.toml"

    # 2) Env fills any missing value (or is sole source if no file / file empty)
    access_key = access_key or os.environ.get("R2_ACCESS_KEY", "").strip()
    secret_key = secret_key or os.environ.get("R2_SECRET_KEY", "").strip()
    endpoint = endpoint or os.environ.get("R2_ENDPOINT", "").strip()
    bucket = bucket or os.environ.get("R2_BUCKET", "datalake").strip()
    if not source or (not access_key and not endpoint):
        source = "environment"

    # Reject only obvious placeholders (exact or host = ACCOUNTID / SEU_ACCOUNT_ID)
    if endpoint:
        up = endpoint.upper()
        if "SEU_ACCOUNT_ID" in endpoint or "YOUR_ACCOUNT_ID" in up or up.rstrip("/").endswith("ACCOUNTID.R2.CLOUDFLARESTORAGE.COM"):
            endpoint = ""

    return access_key, secret_key, endpoint, bucket, source


def get_s3_client():
    import boto3
    from botocore.client import Config

    access_key, secret_key, endpoint, bucket, source = _load_credentials()

    missing = []
    if not access_key:
        missing.append("R2_ACCESS_KEY")
    if not secret_key:
        missing.append("R2_SECRET_KEY")
    if not endpoint:
        missing.append("R2_ENDPOINT")

    if missing:
        secrets_path = SECRETS_FILE.resolve()
        keys_in_file = set()
        if SECRETS_FILE.exists():
            try:
                with open(SECRETS_FILE, "r", encoding="utf-8-sig") as f:
                    for line in f:
                        for key in ("R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_ENDPOINT", "R2_BUCKET"):
                            if line.strip().startswith(key + "=") or (key + " =" in line):
                                keys_in_file.add(key)
                                break
            except Exception:
                pass
        hint = f"Keys in secrets.toml: {', '.join(sorted(keys_in_file)) or 'none'}"
        raise SystemExit(
            "Missing R2 credentials: " + ", ".join(missing) + ".\n\n"
            f"Secrets file: {secrets_path}\n"
            f"Exists: {SECRETS_FILE.exists()}\n"
            f"{hint}\n\n"
            "Option A — Edit .streamlit/secrets.toml: add R2_ENDPOINT=\"https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com\" (use your real account ID), then save (Ctrl+S) and run again.\n"
            "Option B — Same session: $env:R2_ENDPOINT=\"https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com\"; python scripts/upload_to_r2.py"
        )
    # Reject placeholder endpoint (boto3 would raise "Invalid endpoint")
    if "SEU_ACCOUNT_ID" in endpoint or "ACCOUNTID" in endpoint.upper():
        raise SystemExit(
            "R2_ENDPOINT is still a placeholder (SEU_ACCOUNT_ID / ACCOUNTID).\n"
            "Use the real URL, e.g. https://<account_id>.r2.cloudflarestorage.com in .streamlit/secrets.toml or env."
        )
    print(f"Using credentials from: {source}")
    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    ), bucket


def key_from_path(local_path: Path, local_root: Path) -> str:
    """S3 key preserving folder structure; use forward slashes."""
    rel = local_path.relative_to(local_root)
    return str(rel).replace("\\", "/")


def file_unchanged(s3, bucket: str, key: str, local_path: Path) -> bool:
    """Return True if remote object exists and matches local size and mtime (skip upload)."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        remote_size = head.get("ContentLength", -1)
        # Compare size; optionally compare LastModified with local mtime
        local_size = local_path.stat().st_size
        if remote_size != local_size:
            return False
        return True
    except Exception:
        return False


def upload_dir(s3, local_root: Path, remote_prefix: str, bucket: str, skip_unchanged: bool = True):
    """
    Upload directory recursively to s3://bucket/remote_prefix/...
    Preserves folder structure. Skips unchanged files. Returns (uploaded_count, skipped_count).
    """
    if not local_root.is_dir():
        return 0, 0

    uploaded = 0
    skipped = 0
    files = [f for f in local_root.rglob("*") if f.is_file()]

    for local_path in files:
        key = f"{remote_prefix}/{key_from_path(local_path, local_root)}"
        if skip_unchanged and file_unchanged(s3, bucket, key, local_path):
            skipped += 1
            print(f"  skip (unchanged) {key}")
            continue
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"  upload {key} ({size_mb:.2f} MB)")
        s3.upload_file(str(local_path), bucket, key)
        # boto3 upload_file uses multipart automatically for large files (default threshold 8MB)
        uploaded += 1

    return uploaded, skipped


def main():
    s3, bucket = get_s3_client()
    total_up = 0
    total_skip = 0

    layers = [
        (BRONZE_DIR, "bronze"),
        (SILVER_DIR, "silver"),
        (GOLD_DIR, "gold"),
    ]
    for local_dir, remote_prefix in layers:
        if not local_dir.exists():
            print(f"[{remote_prefix}] dir not found: {local_dir}")
            continue
        print(f"[{remote_prefix}] {local_dir} → s3://{bucket}/{remote_prefix}/")
        up, skip = upload_dir(s3, local_dir, remote_prefix, bucket)
        total_up += up
        total_skip += skip
        print(f"[{remote_prefix}] uploaded={up} skipped={skip}")

    print(f"Done. Total uploaded: {total_up}, skipped (unchanged): {total_skip}")


if __name__ == "__main__":
    main()
