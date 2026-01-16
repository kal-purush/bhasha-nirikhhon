import json
import threading
import copy

from securenative.errors import MissingApiKeyError
from securenative.http_client import HttpClient
from securenative.sdk_options import SecureNativeOptions


class QueueItem:
    def __init__(self, url, body):
        self.url = url
        self.body = body


class EventManager:
    def __init__(self, api_key, options=SecureNativeOptions(), http_client=HttpClient()):
        if api_key is None:
            raise MissingApiKeyError()

        self.http_client = http_client
        self.api_key = api_key
        self.options = options
        self.queue = list()

        if self.options.auto_send:
            interval_seconds = max(options.interval // 1000, 1)
            threading.Timer(interval_seconds, self.flush).start()

    def send_async(self, event, resource_path):
        item = QueueItem(
            self._build_url(resource_path),
            json.dumps(event)
        )

        self.queue.insert(0, item)
        if self._is_queue_full():
            self.queue = self.queue[:len(self.queue - 1)]

    def flush(self):
        queue_copy = copy.copy(self.queue)
        self.queue = list()

        for item in queue_copy:
            self.http_client.post(item.url, self.api_key, item.body)

    def send_sync(self, event, resources_path):
        return self.http_client.post(
            self._build_url(resources_path),
            self.api_key,
            json.dumps(event)
        )

    def _build_url(self, resource_path):
        return self.options.api_url + "/" + resource_path

    def _is_queue_full(self):
        return len(self.queue) > self.options.max_events