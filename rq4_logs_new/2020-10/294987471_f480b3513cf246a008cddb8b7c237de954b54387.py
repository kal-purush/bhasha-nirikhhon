from src.padel_requests.base import BaseClient
from src.memoization_decorator import cache_decorator


class Rinconada(BaseClient):
    URL = "http://clubrinconada.cl/booking/srvc.aspx/ObtenerCuadro"

    ID_CUADRO = "5"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",  # pylint: disable=line-too-long
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/json; charset=utf-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "http://clubrinconada.cl",
        "DNT": "1",
        "Connection": "keep-alive",
        "Referer": "http://clubrinconada.cl/Booking/Grid.aspx",
    }

    COOKIES = {
        "ASP.NET_SessionId": "dvqqejq1rqlxpnb5pizpif45",
        "cb-enabled": "enabled",
        "i18next": "es-CL",
    }

    NAME = "Club Rinconada"
    FILTER = "Cancha"

    @cache_decorator("rinconada", 120, index=1)
    def get_schedule(self, date: str):
        return super().get_schedule(date)