#!/usr/bin/python

import re

class TVDScraper():

    def __init__(self):

        self.success = True

        self.title = ''
        self.subtitle = ''
        self.picture = ''
        self.broadcastinfo = ''
        self.channel = ''
        self.date = ''
        self.starttime = ''
        self.endtime = ''
        self.detailURL = ''
        self.ratingValue = '-'
        self.reviewCount = '-'
        self.bestRating = '-'
        self.description = ''
        self.genre = ''
        self.keywords = ''
        self.ratingdata = ''
        self.broadcastflags = ''

    def scrapeHighlights(self, content, contentID):
        try:
            for item in content:
                self.channel = re.compile('/programm/" title="(.+?) Programm"', re.DOTALL).findall(item)[0]
                if re.compile('<span>(.+?)</span>', re.DOTALL).findall(item):
                    self.title = re.compile('<span>(.+?)</span>', re.DOTALL).findall(item)[0]
                else:
                    self.title = re.compile('<h2 class="highlight-title">(.+?)</h2>', re.DOTALL).findall(item)[0]

                _info = re.compile('<a class="highlight-title(.+?)<h2>', re.DOTALL).findall(item)[0]
                self.detailURL = re.compile('href="(.+?)"', re.DOTALL).findall(_info)[0]

                self.date = re.compile('highlight-date">(.+?) | </div>', re.DOTALL).findall(item)[0]
                self.starttime = re.compile('highlight-time">(.+?)</div>', re.DOTALL).findall(item)[0]
                self.genre = re.compile('<strong>(.+?)</strong>', re.DOTALL).findall(item)[0].split('|')[0]
                self.subtitle = re.compile('<strong>(.+?)</strong>', re.DOTALL).findall(item)[1]
        except:
            self.success = False

    def scrapeDetailPage(self, content, contentID):

        if contentID not in content:
            self.success = False
        else:
            self.picture = re.compile('<img id="galpic" itemprop="image" src="(.+?)"', re.DOTALL).findall(content)[0]
            self.title = re.compile('<li id="broadcast-title" itemprop="name">(.+?)</li>', re.DOTALL).findall(content)[0]
            try:
                self.subtitle = re.compile('<li id="broadcast-subtitle"><h2>(.+?)</h2>', re.DOTALL).findall(content)[0]
            except IndexError:
                pass

            # Broadcast details (channel, start, stop)
            bd = re.compile('<li id="broadcast-title" itemprop="name">(.+?)<li id="broadcast-genre', re.DOTALL).findall(content)
            bd = re.compile('<li>(.+?)</li>', re.DOTALL).findall(bd[0])
            try:
                bd = bd[0]
                bd = re.sub('<[^<]+?>','', bd)
                bd = bd.split('|')

                # TVHighlights channel
                self.channel = bd[0]

                # start, stop
                self.starttime = bd[2].split('-')[0].strip()
                self.endtime = bd[2].split('-')[1]

                self.broadcastinfo = '%s: %s - %s' % (self.channel, self.starttime, self.endtime)
            except IndexError:
                pass

            # Ratings
            try:
                self.ratingValue = re.compile('<span itemprop="ratingValue">(.+?)</span>', re.DOTALL).findall(content)[0]
            except IndexError:
                pass

            # Reviews
            try:
                self.reviewCount = re.compile('<span itemprop="reviewCount">(.+?)</span>', re.DOTALL).findall(content)[0]
            except IndexError:
                pass

            # best Ratings
            try:
                self.bestRating = re.compile('<span itemprop="bestRating">(.+?)</span>', re.DOTALL).findall(content)[0]
            except IndexError:
                pass

            # Movie description
            try:
                self.description = re.compile('<span itemprop="description">(.+?)</span>', re.DOTALL).findall(content)[0]
            except IndexError:
                pass

            # Keywords
            try:
                self.keywords = re.compile('<ul class="genre"(.+?)class="desc-block">', re.DOTALL).findall(content)
                kw = re.compile('<span itemprop="genre">(.+?)</span>', re.DOTALL).findall(self.keywords[0])
                if len(kw) == 0:
                    kw = re.compile('<span >(.+?)</span>', re.DOTALL).findall(self.keywords[0])
                self.keywords = kw
            except IndexError:
                pass

            # Rating details
            self.ratingdata = []
            try:
                ratingbox = re.compile('<ul class="rating-box"(.+?)</ul>', re.DOTALL).findall(content)[0].split('<li>')
                ratingbox.pop(0)
                for rating in ratingbox:
                    ratingdict = {'ratingtype': rating.split('span')[0][:-1], 'rating': re.compile('class="rating-(.+?)">', re.DOTALL).findall(rating)}
                    self.ratingdata.append(ratingdict)
            except IndexError:
                self.ratingdata = [{'ratingtype': 'Spannung', 'rating': '-'},
                                  {'ratingtype': 'Action',  'rating': '-'},
                                  {'ratingtype': 'Humor', 'rating': '-'},
                                  {'ratingtype': 'Romantik', 'rating': '-'},
                                  {'ratingtype': 'Sex', 'rating': '-'}]

            # Broadcast Flags
            try:
                bc_info = re.compile('class="broadcast-info-icons tvd-tooltip">(.+?)</ul>', re.DOTALL).findall(content)
                self.broadcastflags = re.compile('class="(.+?)"', re.DOTALL).findall(bc_info[0])
            except IndexError:
                pass

            self.success = True