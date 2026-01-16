__author__ = 'x3mSpeedy'
import urllib2
import urllib
import webbrowser
from mechanize import *
import sys
from bs4 import BeautifulSoup, NavigableString


class Geo(object):
    br = Browser()
    br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; \
          rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
    br.set_handle_robots(False)

    def login(self, username, password):
        self.br.open('https://www.geocaching.com/login/default.aspx')
        self.br.select_form(name="aspnetForm")

        self.br["ctl00$ContentBody$tbUsername"] = username
        self.br["ctl00$ContentBody$tbPassword"] = password
        response = self.br.submit()
        html = response.read()

        if html.find(username) != -1:
            return html

        return None

    def log_cache(self, url, message, date_visited, tb=True):
        self.br.open(url)
        self.br.follow_link(text_regex="Log your visit")

        self.br.select_form(name="aspnetForm")
        self.br.form.set_all_readonly(False)
        self.br["ctl00$ContentBody$LogBookPanel1$ddLogType"] = ["2", ]
        self.br["ctl00$ContentBody$LogBookPanel1$uxDateVisited"] = date_visited
        self.br["ctl00$ContentBody$LogBookPanel1$uxLogInfo"] = message

        if tb:
            self.br[
                "ctl00$ContentBody$LogBookPanel1$uxTrackables$hdnSelectedActions"] = "2045329_Visited,4996238_Visited,"
            self.br["ctl00$ContentBody$LogBookPanel1$uxTrackables$repTravelBugs$ctl01$ddlAction"] = [
                '2045329_Visited', ]
            self.br["ctl00$ContentBody$LogBookPanel1$uxTrackables$repTravelBugs$ctl02$ddlAction"] = [
                '4996238_Visited', ]

        response = self.br.submit()
        html = response.read()
        return html


if __name__ == "__main__":
    urls = [
        "http://www.geocaching.com/geocache/GC4JW3M_cyklostezka-varhany-oldrichovska-vi",
        "http://www.geocaching.com/geocache/GC4JW6G_cyklostezka-varhany-oldrichovska-xi",
        "http://www.geocaching.com/geocache/GC4JW62_cyklostezka-varhany-oldrichovska-x",
        "http://www.geocaching.com/geocache/GC4JW4Z_cyklostezka-varhany-oldrichovska-ix",
        "http://www.geocaching.com/geocache/GC4JW4R_cyklostezka-varhany-oldrichovska-viii",
        "http://www.geocaching.com/geocache/GC4JW4A_cyklostezka-varhany-oldrichovska-vii"
            ]
    message = "O vikendu jsme podnikli mensi vylet a padla nam do oka tahle serie. Udelali jsme jeji prvni polovinu, ale urcite prijdeme udelat i zbytek. Moc diky za peknou prochazku. Geohafanovi se tu take moc libilo. A jeste diky za pokoreni 400 kese!"
    g = Geo()
    print 'login'
    g.login('x3mSpeedy', '')

    for link in urls:
        print 'logging: ' + link
        g.log_cache(url=link, message=message, date_visited="20.Mar.2015")