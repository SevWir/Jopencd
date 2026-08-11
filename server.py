import socket

HOST = "0.0.0.0"
PORT = 5055

with socket.create_server((HOST, PORT)) as server:
    print(f"SWI server started on {HOST}:{PORT}", flush=True)

    while True:
        client, address = server.accept()

        print(f"Connection from {address}", flush=True)

        try:
            client.sendall(b"SWI_ONLINE\n")
        finally:
            client.close()