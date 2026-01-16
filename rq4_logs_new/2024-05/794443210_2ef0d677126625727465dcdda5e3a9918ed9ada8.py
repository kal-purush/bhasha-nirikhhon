from locust import task, run_single_user, between
from locust import FastHttpUser


class qr_2(FastHttpUser):
    host = "http://34.29.225.36"
    wait_time = between(1, 2)
    @task
    def t(self):
        self.client.get("/")
        self.client.get("/forgot.html")
        self.client.get("/reset.html")
        with self.client.request(
            "GET",
            "/qr-generator.html",
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
                "Cache-Control": "max-age=0",
                "Connection": "keep-alive",
                "DNT": "1",
                "Host": "34.29.225.36",
                "If-Modified-Since": "Tue, 30 Apr 2024 17:12:56 GMT",
                "If-None-Match": 'W/"66312698-927"',
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            },
            catch_response=True,
        ) as resp:
            pass
        with self.client.request(
            "POST",
            "https://faas.thu4n.dev/function/qrcode-go",
            headers={
                "accept": "*/*",
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "en-US,en;q=0.9,vi;q=0.8",
                "content-type": "text/plain",
                "dnt": "1",
                "origin": "http://34.29.225.36",
                "priority": "u=1, i",
                "referer": "http://34.29.225.36/",
                "sec-ch-ua": '"Chromium";v="124", "Microsoft Edge";v="124", "Not-A.Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "cross-site",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            },
            data="facebook.com",
            catch_response=True,
        ) as resp:
            pass


if __name__ == "__main__":
    run_single_user(qr_2)