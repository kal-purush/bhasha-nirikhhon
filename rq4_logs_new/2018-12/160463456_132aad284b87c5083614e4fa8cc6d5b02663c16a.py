from bs4 import BeautifulSoup

class LotData:
    '''
    Extract data from lot information page (from MainContentImage, MainContentDetails
    and lotDetails divs of MainContent div)
    
    Data to collect from lot info:
     - Image
     
     - Lot #
     - Artist name
     - Title
     - Date of birth/death
     - Price sold
     - Estimate
     
     - Artist signature
     - Medium
     - Size
     - Year of creation (painted, conveived, etc.)
     - Provenance
    '''
    
    def __init__(self, lot_html, ids):
        self.soup = BeautifulSoup(lot_html)
        
        self.ids = ids
        
        details = soup.find(id = "main_center_0_lblLotDescription").text
        details_as_list = [i.strip() for i in details.strip().split('\n')]
    
    def get_image(self):
        image_section = self.soup.find(id='main_center_0_imgCarouselMain')
        return image_section.find('img').get('src')
    
    def get_lot_no(self):
        return int(self.soup.find(id='main_center_0_lblLotNumber').strip())
    
    def get_artist(self):
        primary_title = self.soup.find(id="main_center_0_lblLotPrimaryTitle").text
        artist_name_split = primary_title.strip().split()[:-1]

        return ' '.join(artist_name_split)
    
    def get_title(self):
        lot_title = self.soup.find(id="main_center_0_lblLotSecondaryTitle").text
        return lot_title.strip()
    
    def get_date_of_birth(self):
        primary_title = self.get_primary_title()
        dates = primary_title.split()[-1][1:-1].split('-')
        return int(dates[0])
    
    def get_date_of_death(self):
        primary_title = self.get_primary_title()
        dates = primary_title.split()[-1][1:-1].split('-')
        return int(dates[1])
    
    def get_primary_title(self):
        primary_title = self.soup.find(id="main_center_0_lblLotPrimaryTitle").text
        return primary_title.strip()
    
    def get_price_sold(self):
        price = self.soup.find(id="main_center_0_lblPriceRealizedPrimary").text
        return price.strip()
    
    def get_estimate(self):
        estimate = self.soup.find(id="main_center_0_lblPriceEstimatedPrimary").text
        return estimate.strip()
    
    def get_artist_signature(self):        
        return self.lot_details[3]
    
    def get_medium(self):
        return self.lot_details[4]
    
    def get_size(self):
        if len(self.lot_details) == 6:
            return self.lot_details[4]
        
        return self.lot_details[4:5]
    
    def get_year_of_creation(self):
        return self.lot_details[-1]
    
    def get_provenence(self):
        return self.soup.find(id='main_center_0_lblLotProvenance').text
    