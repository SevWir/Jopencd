import socket
import json
import subprocess


HOST = "0.0.0.0"
PORT = 5055


def get_stats(steamid):
    result = subprocess.run(
        ["node", "stats.js", steamid],
        capture_output=True,
        text=True,
        timeout=10
    )

    if result.returncode != 0:
        return {
            "ok": False,
            "error": result.stderr or "stats.js failed"
        }

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {
            "ok": False,
            "error": "invalid_stats_response"
        }


with socket.create_server((HOST, PORT)) as server:
    print(f"SWI server started on {HOST}:{PORT}", flush=True)

    while True:
        client, address = server.accept()

        print(f"Connection from {address}", flush=True)

        try:
            data = client.recv(4096).decode("utf-8").strip()

            if not data:
                client.sendall(b'{"ok":false,"error":"empty_request"}\n')
                continue

            request = json.loads(data)

            action = request.get("action")

            if action == "ping":
                response = {
                    "ok": True,
                    "server": "SWI_ONLINE"
                }

            elif action == "stats":
                steamid = str(request.get("steamid", ""))

                if not steamid.isdigit():
                    response = {
                        "ok": False,
                        "error": "invalid_steamid"
                    }
                else:
                    response = get_stats(steamid)

            else:
                response = {
                    "ok": False,
                    "error": "unknown_action"
                }

            client.sendall(
                (json.dumps(response) + "\n").encode("utf-8")
            )

        except Exception as e:
            response = {
                "ok": False,
                "error": str(e)
            }

            try:
                client.sendall(
                    (json.dumps(response) + "\n").encode("utf-8")
                )
            except:
                pass

        finally:
            client.close()