from bs4 import BeautifulSoup
from model import OpeningHours


class OpeningHoursParser:
    @staticmethod
    def parse_html(raw_html):
        """
        HTMLを受け取り開園時間情報を返す。

        Returns:
        --------
        opening_hours : Obj
            開園時間情報クラスオブジェクト
        """
        soup = BeautifulSoup(raw_html, "html.parser")
        time_class_list = soup.find_all(class_='time')
        # 現在の仕様では先頭のものがパークの営業時間
        opening_hours_str = time_class_list[0].text
        opening_hours = OpeningHours(opening_hours_str)
        return opening_hours