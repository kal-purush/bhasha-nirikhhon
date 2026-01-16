"""Cowrie API output plugin."""
# Copyright 2016 Russell Troxel
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json
import base64

from requests import Session, Request

import cowrie.core.output

# entry = {
#     "entryid": "asdf",
#     "session": "asdf",
#     "timestamp": "isoformat",
#     "src_ip": "1.2.3.4",
#     "username": "asdf",
#     "password": "asdf",
#     "input": "cmd",
#     "url": "asdf",
#     "outfile": "asdf",
#     "shasum": "asdf",
#     "realm": "asdf",
#     "version": "asdf",
#     "width": 1,
#     "height": 1,
#     "ttylog": "asdf",
#     "size": "asdf",
#     "fingerprint": "asdf"
# }


class HttpStatusCodeError(Exception):
    """Error when status code was not expected."""

    def __init__(self, response):
        """Initialize error from requests Response."""
        msg = "Unexpected Response from Server: HTTP {0}, Body: {1}"
        self.message = msg.format(response.status_code, response.text)
        self.resp = response

    def __str__(self):
        """Return String."""
        return repr(self.message)


class Output(cowrie.core.output.Output):
    """Output over REST to the cowrie_api."""

    def __init__(self, cfg):
        """Init."""
        self.cfg = cfg
        self.token = cfg.get('output_donthackme', "token")
        self.endpoint = cfg.get('output_donthackme', "endpoint")
        cowrie.core.output.Output.__init__(self, cfg)

    def headers(self):
        """Prepare request headers."""
        return {"X-Auth-Token": self.token, "Content-Type": "application/json"}

    def make_url(self, path):
        """Join base endpoint and path."""
        return "".join((self.endpoint, "/events", path))

    def prepare_entry(self, logentry, payload, path, method="PUT", popkeys=[]):
        """Prepare an entry based on base_data."""
        data = {
            "method": method,
            "url": self.make_url(path),
            "data": {
                "session": logentry["session"],
                "sensor_name": self.sensor,
                "timestamp": logentry["timestamp"]
            }
        }
        if "dst_ip" in logentry:
            data["data"]["sensor_ip"] = logentry["dst_ip"]
        data["data"].update(payload)

        for key in popkeys:
            data["data"].pop(key)

        return data

    def send_data(self, method, url, data):
        """Send data to endpoint."""
        req = Request(
            method.upper(),
            url,
            data=json.dumps(data),
            headers=self.headers()
        )

        s = Session()
        prepped = s.prepare_request(req)

        resp = s.send(prepped)
        if resp.status_code not in [201, 202]:
            raise HttpStatusCodeError(resp)
        return resp.json()

    def start(self):
        """Start."""
        pass

    def stop(self):
        """Stop."""
        pass

    def write(self, logentry):
        """Class write method."""
        prepared_entries = []

        if logentry["eventid"] == "cowrie.session.connect":

            payload = {
                "source_ip": logentry["src_ip"],
                "start_time": logentry["timestamp"]
            }

            data = self.prepare_entry(
                logentry,
                payload,
                "/session/connect",
                method="POST",
                popkeys=["timestamp"]
            )

            prepared_entries.append(data)

        elif logentry["eventid"] in ["cowrie.login.success",
                                     "cowrie.login.failed"]:

            payload = {
                "username": logentry["username"],
                "password": logentry["password"],
                "success": False
            }

            path = "/login/failed"
            if logentry["eventid"] == "cowrie.login.success":
                payload["success"] = True
                path = "/login/success"

            data = self.prepare_entry(
                logentry,
                payload,
                path
            )
            prepared_entries.append(data)

        elif logentry["eventid"] in ["cowrie.command.success",
                                     "cowrie.command.failed"]:
            payload = {
                "command": logentry["input"],
                "success": False
            }
            path = "/command/failed"

            if logentry["eventid"] == "cowrie.command.success":
                payload["success"] = True
                path = "/command/success"

            data = self.prepare_entry(
                logentry,
                payload,
                path
            )
            prepared_entries.append(data)

        elif logentry["eventid"] == "cowrie.session.file_download":
            payload = {
                "shasum": logentry["shasum"],
                "url": logentry["url"],
                "outfile": logentry["outfile"]
            }
            data = self.prepare_entry(
                logentry,
                payload,
                "/session/file_download"
            )
            prepared_entries.append(data)

        elif logentry["eventid"] == 'cowrie.client.version':
            payload = {
                "ssh_version": logentry["version"],
                "ssh_kexAlgs": logentry["kexAlgs"],
                "ssh_keyAlgs": logentry["keyAlgs"],
                "ssh_macCS": logentry["macCS"]
            }
            data = self.prepare_entry(
                logentry,
                payload,
                "/client/version",
                popkeys=["timestamp"]
            )
            prepared_entries.append(data)

        elif logentry["eventid"] == 'cowrie.client.size':
            payload = {
                "ttysize": {
                    "height": logentry["height"],
                    "width": logentry["width"],
                }
            }

            data = self.prepare_entry(
                logentry,
                payload,
                "/client/size",
                popkeys=["timestamp"]
            )
            prepared_entries.append(data)

        elif logentry["eventid"] == 'cowrie.session.closed':
            data = self.prepare_entry(
                logentry,
                {"end_time": logentry["timestamp"]},
                "/session/closed",
                popkeys=["timestamp"]
            )
            prepared_entries.append(data)

        elif logentry["eventid"] == 'cowrie.log.closed':
            max_size = logentry["size"] + 1024
            with open(logentry["ttylog"], "rb") as f:
                ttylog = f.read(max_size)

            payload = {
                "ttylog": {
                    "size": logentry["size"],
                    "log_location": logentry["ttylog"],
                    "log_base64": base64.b64encode(ttylog)
                }
            }
            data = self.prepare_entry(
                logentry,
                payload,
                "/log/closed",
                popkeys=["timestamp"]

            )
            prepared_entries.append(data)

        elif logentry["eventid"] == 'cowrie.client.fingerprint':
            payload = {
                "username": logentry["username"],
                "fingerprint": logentry["fingerprint"]
            }
            data = self.prepare_entry(
                logentry,
                payload,
                "/client/fingerprint"
            )
            prepared_entries.append(data)

        # elif logentry["eventid"] == "cowrie.direct-tcpip.request":
        #     payload = {
        #         "dest_port": logentry["dst_port"],
        #         "dest_ip": logentry["dst_ip"]
        #     }
        #     data = self.prepare_entry(
        #         logentry,
        #         payload,
        #         "/cdirect-tcpip/request"
        #     )
        #     prepared_entries.append(data)

        for entry in prepared_entries:
            self.send_data(**entry)