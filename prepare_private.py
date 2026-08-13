import base64
import io
import os
import sys
import zipfile
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PRIVATE_DIR = BASE_DIR / "private"
MAFILES_DIR = PRIVATE_DIR / "maFiles"
DATA_DIR = PRIVATE_DIR / "data"
BUNDLE_ENV = "SWI_PRIVATE_BUNDLE_B64"


def _safe_member_path(base: Path, member_name: str) -> Path:
    target = (base / member_name).resolve()
    base_resolved = base.resolve()
    if target != base_resolved and base_resolved not in target.parents:
        raise RuntimeError(f"Unsafe ZIP path: {member_name}")
    return target


def prepare_private_files() -> None:
    bundle = os.environ.get(BUNDLE_ENV, "").strip()
    if not bundle:
        raise RuntimeError(f"{BUNDLE_ENV} is not configured in Railway.")

    try:
        raw_zip = base64.b64decode(bundle, validate=True)
    except Exception as exc:
        raise RuntimeError(f"{BUNDLE_ENV} contains invalid Base64.") from exc

    PRIVATE_DIR.mkdir(parents=True, exist_ok=True)
    MAFILES_DIR.mkdir(parents=True, exist_ok=True)
    # Important: do NOT delete private/data. It may contain refresh tokens and
    # steam-user machine data that should survive restarts when /app/private
    # is backed by a Railway Volume.
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Replace only the bundle-managed inputs (logpass + maFiles).
    for old_file in MAFILES_DIR.glob("*.maFile"):
        try:
            old_file.unlink()
        except OSError:
            pass

    with zipfile.ZipFile(io.BytesIO(raw_zip), "r") as archive:
        for info in archive.infolist():
            name = info.filename.replace("\\", "/").lstrip("/")
            if not name or name.endswith("/"):
                continue

            # We intentionally extract only the files that come from the bundle.
            # Never overwrite private/data from the bundle.
            if name == "logpass.txt":
                destination = PRIVATE_DIR / "logpass.txt"
            elif name.startswith("maFiles/"):
                destination = _safe_member_path(PRIVATE_DIR, name)
            else:
                continue

            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info, "r") as src, open(destination, "wb") as dst:
                dst.write(src.read())

    logpass = PRIVATE_DIR / "logpass.txt"
    if not logpass.is_file():
        raise RuntimeError("private/logpass.txt was not created.")

    mafiles = list(MAFILES_DIR.glob("*.maFile"))
    if not mafiles:
        raise RuntimeError("No maFiles were found in private/maFiles.")

    print(f"Private worker data prepared. maFiles: {len(mafiles)}", flush=True)
    print(f"Persistent auth data directory: {DATA_DIR}", flush=True)


def start_server() -> None:
    server_file = BASE_DIR / "server.py"
    config_file = BASE_DIR / "server-config.json"

    if not server_file.is_file():
        raise RuntimeError("server.py not found.")
    if not config_file.is_file():
        raise RuntimeError("server-config.json not found.")

    os.execv(
        sys.executable,
        [sys.executable, str(server_file), "--config", str(config_file)],
    )


def main() -> None:
    try:
        prepare_private_files()
        start_server()
    except Exception as exc:
        print(f"STARTUP ERROR: {exc}", flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
