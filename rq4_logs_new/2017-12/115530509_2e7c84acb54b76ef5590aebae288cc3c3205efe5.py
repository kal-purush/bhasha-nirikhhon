import requests
from BeautifulSoup import BeautifulSoup
import tempfile
import zipfile
import os
from constants import START_URL, USER_AGENT, BASE_URL


class BseWebScraper():
    def download_equity_csv(self):
        """download zip file, unzip file and store it locally"""
        url = self._get_download_url()
        resp = requests.get(url, stream=True)
        path = self._store_stream_locally(resp)
        file_names = self._unzip_file(path)

        return file_names[0]

    def _store_stream_locally(self, s, suffix='.zip'):
        """store zip file locally """
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as foutp:
            for chunk in s.iter_content(chunk_size=1024 * 1024):
                foutp.write(chunk)
            foutp.flush()
            return foutp.name

    def _unzip_file(self, path):
        """Unzip give path file"""
        zip_file = zipfile.ZipFile(path)
        file_names = zip_file.namelist()
        zip_file.extractall("/tmp/")
        self.delete_file(path)
        return file_names

    def _get_download_url(self):
        """Extract download url from give url"""
        headers = {}
        headers['User-Agent'] = USER_AGENT
        resp = requests.get(START_URL, headers=headers)
        b_html = BeautifulSoup(resp.content)
        src = b_html.find("iframe")['src']

        url = BASE_URL + str(src)

        resp = requests.get(url, headers=headers)
        b_html = BeautifulSoup(resp.content)

        url = b_html.find(id="btnhylZip")["href"]
        return url

    def delete_file(self, path):
        """Delete file on given path"""
        try:
            os.remove(path)
        except OSError:
            pass