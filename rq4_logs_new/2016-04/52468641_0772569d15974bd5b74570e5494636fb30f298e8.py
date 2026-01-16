# fluxs download

from classes.flux.flux import flux
from classes.flux.flux_manager import flux_manager
from modules.flux_download.flux_download import flux_download

import config

class fluxs_download():
    
    """downlad every available flux"""

    def __init__(self):
        self.conf = config.get_conf()
		
    def run(self):
        
        self.flux_mng = flux_manager()
        self.flux_list = self.flux_mng.get_all()
        
        success = True
        clear = True        
        report = ''
        show_flux_list = True
        
        for flux in self.flux_list:
            flux_downloader = flux_download()
            download_results = flux_downloader.run(flux.id) #[True, True, str(flux.id), True]
            
            success = success and download_results[0]
            clear =  success and download_results[1]
            report += download_results[2] + "\n\n" 
            show_flux_list = success and download_results[3]
            
        return [success, clear, report, show_flux_list]
            
            
	    