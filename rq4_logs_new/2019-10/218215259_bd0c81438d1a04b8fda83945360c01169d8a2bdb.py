import argparse
import dataclasses
import logging
import socket
import ssl

from typing import Dict, Optional


@dataclasses.dataclass
class HTTPResponse:
    status_code: int
    status_text: str
    headers: Dict[str, str] = dataclasses.field(default_factory=dict)
    body: str = None


def fetch(hostname: str, port=443) -> Optional[HTTPResponse]:
    context = ssl.create_default_context()
    with socket.create_connection((hostname, port)) as s:
        with context.wrap_socket(s, server_hostname=hostname) as ss:
            # XXX: This fails (hangs at 'recv') if HTTP/1.1 is used instead. Why?
            request = f"GET / HTTP/1.0\r\nHost: {hostname}\r\n\r\n"
            ss.send(request.encode())
            data = b""
            while True:
                recv = ss.recv(1024)
                if not recv:
                    break
                data += recv
            response = parse_http_response(data)
    return response


def parse_http_response(data: bytes) -> Optional[HTTPResponse]:
    response = None
    lines = data.split(b"\r\n")

    status_line = lines.pop(0)
    if not status_line.startswith(b"HTTP/"):
        print("Invalid HTTP response!")
        return

    _, status_code, status_text = status_line.split(b" ", 2)

    headers = {}
    header_line = lines.pop(0)
    while header_line:
        # XXX: Will there always be a space after the colon delimiter?
        name, value = header_line.decode().split(": ")
        headers[name] = value
        header_line = lines.pop(0)

    body = None
    if lines:
        body = b"\r\n".join(lines).decode()

    response = HTTPResponse(int(status_code), status_text, headers, body)
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname")
    args = parser.parse_args()
    response = fetch(args.hostname)
    if response is None:
        print(f"No response received from https://{args.hostname}.")
    else:
        print(f"Response received from {args.hostname}:\r\n")
        print(f"Status code: {response.status_code}\r\n")
        print(f"Headers: {response.headers}\r\n")
        if response.body:
            print(f"Body:\r\n{response.body}")