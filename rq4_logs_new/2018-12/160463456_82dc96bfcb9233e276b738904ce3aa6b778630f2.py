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
    
    def __init__(self, lot_html):
        self.soup = BeautifulSoup(lot_html, 'html.parser')

        self.lot_details = self.get_lot_details()

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,
                              self.get_lot_no())

    def lot_data_as_list(self):

        data = []

        data.append(self.get_lot_no())
        data.append(self.get_artist())
        data.append(self.get_title())
        data.append(self.get_year_of_birth())
        data.append(self.get_year_of_death())
        data.append(self.get_price_sold())
        data.append(self.get_estimate())
        data.append(self.get_artist_signature())
        data.append(self.get_medium())
        
        dim0, dim1 = self.get_size()
        data.append(dim0)
        data.append(dim1)
        
        data.append(self.get_year_of_creation())
        data.append(self.get_provenence())
        data.append(self.get_image())

        return data
    
    def get_image(self):
        image_section = self.soup.find(id='main_center_0_imgCarouselMain')
        return image_section.find('img').get('src')
    
    def get_lot_no(self):
        return self.soup.find(id='main_center_0_lblLotNumber').text.strip()
    
    def get_artist(self):
        primary_title = self.get_primary_title()
        if 'b.' in primary_title:
            artist_name_split = primary_title.strip().split()[:-2]
        else:
            artist_name_split = primary_title.strip().split()[:-1]

        return ' '.join(artist_name_split)
    
    def get_title(self):
        lot_title = self.soup.find(id="main_center_0_lblLotSecondaryTitle").text
        return lot_title.strip()
    
    def get_year_of_birth(self):
        primary_title = self.get_primary_title()
        
        if 'b.' in primary_title:
            return primary_title.split()[-1].replace(')', '')
        
        years = primary_title.split()[-1][1:-1].split('-')
        return years[0]
    
    def get_year_of_death(self):
        primary_title = self.get_primary_title()
        if 'b.' in primary_title:
            return 'N.A.'
        years = primary_title.split()[-1][1:-1].split('-')
        return years[1]
    
    def get_price_sold(self):
        price = self.soup.find(id="main_center_0_lblPriceRealizedPrimary").text
        return price.strip()
    
    def get_estimate(self):
        estimate = self.soup.find(id="main_center_0_lblPriceEstimatedPrimary").text
        return estimate.strip()
    
    def get_artist_signature(self):
        #if len(self.lot_details) == 7:
        #    return 'N.A.'
        
        if 'signed' not in self.lot_details[2]:
            return 'N.A.'
        
        return self.lot_details[2]
    
    def get_medium(self):
        if 'signed' not in self.lot_details[2]:
            return self.lot_details[2]
        
        return self.lot_details[3]
    
    def get_size(self):
        if len(self.lot_details) <= 6:
            create_words = ('Painted','Conceived', 'Executed')
            if not any(s in self.lot_details[-1] for s in create_words):
                dimensions = self.lot_details[-1].split()[-4:-1]
                length = dimensions[-3].strip('(')
                width = dimensions[-1]

                return length, width
                
            if 'x' not in self.lot_details[-2]:
                dimensions = self.lot_details[-2].split()[-4:-1]
                return (dimensions[-1].strip('('), 'N.A.')
            
            dimensions = self.lot_details[-2].split()[-4:-1]
            length = dimensions[-3].strip('(')
            width = dimensions[-1]
            
            return length, width
        
        dimensions = self.lot_details[4:6]
        values = []
        for dim in dimensions:
            dim_value = dim.strip().split()[-2].strip('(')
            values.append(dim_value)
            
        return tuple(values)
    
    def get_year_of_creation(self):
        create_words = ('Painted','Conceived', 'Executed')
        if not any(s in self.lot_details[-1] for s in create_words):
            return 'N.A.'
        return self.lot_details[-1]
    
    def get_provenence(self):
        return self.soup.find(id='main_center_0_lblLotProvenance').text.replace('\r\n', ' ')

    def get_primary_title(self):
        primary_title = self.soup.find(id="main_center_0_lblLotPrimaryTitle").text
        return primary_title.strip()

    def get_lot_details(self):
        text = self.soup.find(id = "main_center_0_lblLotDescription").prettify()
        text = text.replace('<span id="main_center_0_lblLotDescription">', '')
        text = text.replace('</span>', '')
        text = text.replace('\n', '')
        text = text.replace('</i>', '').replace('<i>', '')
        text = text.strip().split("<br/>")[:-1]
        details_as_list = [i.strip() for i in text]

        return details_as_list