class NflScraping:
        def __init__(self, stockPrefix):
            self.baseUrl = "https://www.kauppalehti.fi/porssi/porssikurssit/osake/"
            self.stockPrefix = stockPrefix