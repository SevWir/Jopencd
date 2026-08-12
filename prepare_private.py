import os
import sys
import io
import base64
import zipfile
import shutil


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PRIVATE_DIR = os.path.join(
    BASE_DIR,
    "private"
)

BUNDLE_ENV = "SWI_PRIVATE_BUNDLE_B64"


def safe_extract_zip(zip_file, destination):
    destination_real = os.path.realpath(destination)

    for member in zip_file.infolist():
        member_path = os.path.realpath(
            os.path.join(destination, member.filename)
        )

        if not (
            member_path == destination_real
            or member_path.startswith(destination_real + os.sep)
        ):
            raise RuntimeError(
                f"Unsafe ZIP path: {member.filename}"
            )

    zip_file.extractall(destination)


def prepare_private_files():
    bundle = os.environ.get(BUNDLE_ENV, "").strip()

    if not bundle:
        raise RuntimeError(
            f"{BUNDLE_ENV} is not configured in Railway."
        )

    try:
        raw_zip = base64.b64decode(
            bundle,
            validate=True
        )
    except Exception as error:
        raise RuntimeError(
            "SWI_PRIVATE_BUNDLE_B64 contains invalid Base64."
        ) from error

    if os.path.isdir(PRIVATE_DIR):
        shutil.rmtree(PRIVATE_DIR)

    os.makedirs(
        PRIVATE_DIR,
        exist_ok=True
    )

    try:
        with zipfile.ZipFile(
            io.BytesIO(raw_zip),
            "r"
        ) as archive:
            safe_extract_zip(
                archive,
                PRIVATE_DIR
            )
    except Exception as error:
        raise RuntimeError(
            f"Could not unpack private bundle: {error}"
        ) from error

    logpass = os.path.join(
        PRIVATE_DIR,
        "logpass.txt"
    )

    mafiles = os.path.join(
        PRIVATE_DIR,
        "maFiles"
    )

    if not os.path.isfile(logpass):
        raise RuntimeError(
            "private/logpass.txt was not created."
        )

    if not os.path.isdir(mafiles):
        raise RuntimeError(
            "private/maFiles was not created."
        )

    mafile_count = len([
        name
        for name in os.listdir(mafiles)
        if name.lower().endswith(".mafile")
    ])

    if mafile_count == 0:
        raise RuntimeError(
            "No maFiles were found."
        )

    print(
        f"Private worker data prepared. "
        f"maFiles: {mafile_count}",
        flush=True
    )


def start_server():
    server_file = os.path.join(
        BASE_DIR,
        "server.py"
    )

    config_file = os.path.join(
        BASE_DIR,
        "server-config.json"
    )

    if not os.path.isfile(server_file):
        raise RuntimeError(
            "server.py not found."
        )

    if not os.path.isfile(config_file):
        raise RuntimeError(
            "server-config.json not found."
        )

    os.execv(
        sys.executable,
        [
            sys.executable,
            server_file,
            "--config",
            config_file,
        ],
    )


def main():
    try:
        prepare_private_files()
        start_server()

    except Exception as error:
        print(
            f"STARTUP ERROR: {error}",
            flush=True
        )
        sys.exit(1)


if __name__ == "__main__":
    main()