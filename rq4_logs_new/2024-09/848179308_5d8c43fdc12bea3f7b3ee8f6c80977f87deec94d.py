import time
import threading
from urllib.parse import urljoin
import requests
import backoff

from .settings import SETTINGS
from .concurrency import Concurrency
from .log import logger


class Heartbeat:
    _thread_started = False

    def __init__(self, concurrency: Concurrency) -> None:
        self._session = requests.Session()
        self._concurrency = concurrency
        self._heartbeat_url = urljoin(SETTINGS.agent_url(), "/apis/v1/heartbeat")

    def start(self):
        if not Heartbeat._thread_started:
            hb = threading.Thread(target=self._run_heartbeat, daemon=True)
            hb.start()
            Heartbeat._thread_started = True

    def _run_heartbeat(self):
        """
        Sends heartbeat pings to the Runpod server.
        """
        interval = SETTINGS.heartbeat_interval()
        while True:
            self._do_heartbeat()
            time.sleep(interval)

    def _do_heartbeat(self):
        jobs = self._concurrency.get_jobs()

        @backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=3,
        )
        def do_send():
            result = self._session.post(self._heartbeat_url, json={"requestIDs": jobs})
            logger.debug(f"heartbeat status: {result.status_code}")

        try:
            do_send()
        except Exception as e:
            logger.error(f"failed to send heartbeat: {e}", exc_info=True)