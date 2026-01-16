from services.redis_services import RedisServices
from services.constants import HTML_TEMPLTE


class ViewBHAVCopy():
    def __init__(self):
        self.rdb = RedisServices()

    def get_top_ten_view(self):
        """return view for top ten companies"""
        companies_details = self.rdb.get_top_ten()
        return self._page_view(self._print_table(companies_details))

    def get_by_name_view(self, company_name):
        """Return view for search """
        company_details = self.rdb.get_by_name(company_name)
        return self._page_view(self._print_table(
            [company_details]) if company_details else "")

    def _print_table(self, companies_details):
        html = """<table class="table"> <th>Code</th> <th>Name</th> <th>Open</th>\
        <th>High</th> <th>Low</th> <th>Close</th>"""
        for company_details in companies_details:
            html += '<tr> <td> %s </td> <td> %s </td>  <td> %s </td> <td> %s </td>\
            <td> %s </td> <td> %s </td></tr>' % (company_details["SC_CODE"],
                                                 company_details["SC_NAME"],
                                                 company_details["OPEN"],
                                                 company_details["HIGH"],
                                                 company_details["LOW"],
                                                 company_details["CLOSE"])

        html += '</table>'
        return html

    def _page_view(self, table_html):
        html = HTML_TEMPLTE % (table_html)
        return html