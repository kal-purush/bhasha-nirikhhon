import time
  
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.select import Select

class ConsumeSite(object):
    FIELDS_TO_IGNORE = ("XX")
    CATEGORY_FIELD = "grupo"
    PERIOD_FIELD = "periodo"
    REGION_FIELD = "CCAA"

    def __init__(self, url, input_file = None, output_file = None):
        options = Options()
        options.add_argument('--headless')
        self.driver = webdriver.Firefox(options=options)
        self.driver.get(url)

    def __del__(self):
        self.driver.quit()

    def get_data(self):
        categories = self.__get_product_categories()
        periods = self.__get_periods()
        regions = self.__get_regions()
        print("There are {0} categories, {1} periods and {2} regions what makes a total of {3} requests".format(len(categories), len(periods), len(regions), len(categories) * len(periods) * len(regions)))
        data = self.__request_all_data(categories, periods, regions)
        print("The generated data has {0} registries".format(len(data)))
        return data

    def __get_product_categories(self):
        return self.__get_selector_options(self.CATEGORY_FIELD)

    def __get_periods(self):
        return self.__get_selector_options(self.PERIOD_FIELD)

    def __get_regions(self):
        return self.__get_selector_options(self.REGION_FIELD)

    def __get_selector_options(self, selector_name):
        options = []
        selector = Select(self.driver.find_element_by_name(selector_name))
        for option in selector.options:
            if option.get_attribute("value") in self.FIELDS_TO_IGNORE:
                continue
            options.append(option.text)
        return options
    
    def __request_all_data(self, categories, periods, regions):
        data = [] 
        for category in categories:
            for period in periods:
                for region in regions:
                    print("Options to request: \n Category: {0}\n Period: {1}\n Region: {2}".format(category, period, region))
                    data.extend(self.__request_data(category, period, region))
        return data
    
    def __select_options(self, options):
        for key, value in options.items():
            element = Select(self.driver.find_element_by_name(key))
            element.select_by_visible_text(value) #select_by_value(value)
            
    def __request_data(self, category, period, region):
        data = []
        self.__select_options({self.CATEGORY_FIELD: category, self.PERIOD_FIELD: period, self.REGION_FIELD: region})
        self.driver.find_element_by_name("boton1").click()
                
        aditional_data = \
            {"Categoría": category,
             "Mes": period.split("/")[0],
             "Año": period.split("/")[1],
             "Región": region,}
        raw_data = self.__parse_data(self.driver.find_element_by_xpath("/html/body/div/div/div[1]/div[2]/div/div[2]/div/div[13]/div/table[2]/tbody"))
        for registry in raw_data:
            registry.update(aditional_data)
            data.append(registry)
        self.driver.back()
        return data

    def __parse_data(self, element):
        registries = []
        fields = []
        for row in element.find_elements_by_xpath(".//tr"):
            if len(fields) == 0:
                for data in row.find_elements_by_xpath(".//td"):
                    fields.append(data.text)
            else:
                c = 0
                registry = {}
                for data in row.find_elements_by_xpath(".//td"):
                    if c != 0:
                        registry[fields[c]] = self.__numerify(data.text)
                    else:
                        registry[fields[c]] = data.text
                    c += 1
                registries.append(registry)
        # print("Data saved: {0}".format(registries))
        return registries

    def __numerify(self, string):
        string = string.replace(".", "")
        string = string.replace(",", ".")
        if string == "":
            string = "0"
        return float(string)
        