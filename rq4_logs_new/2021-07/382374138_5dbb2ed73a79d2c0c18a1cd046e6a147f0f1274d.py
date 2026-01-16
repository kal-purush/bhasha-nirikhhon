import scrapy
import numpy as np
import re
import time

class TitleSpider(scrapy.Spider):
    name = 'data_analyst'
    allowed_domains = ['seek.com.au']

    def start_requests(self):
        links= [
            'https://www.seek.com.au/data-analyst-jobs/in-All-Australia?salaryrange=0-80000&salarytype=annual',
            'https://www.seek.com.au/data-analyst-jobs/in-All-Australia?salaryrange=80000-120000&salarytype=annual',
            'https://www.seek.com.au/data-analyst-jobs/in-All-Australia?salaryrange=120000-999999&salarytype=annual',
        ]
        for index, link in enumerate(links):
            yield scrapy.Request(url=link, callback=self.parse, cb_kwargs = {'index': index})


    def parse(self, response, index):
        if index == 0:
            sal_range = 'low'
        elif index == 1:
            sal_range ='mid'
        elif index == 2:
            sal_range = 'high'

        urls = response.xpath('//*[@class="_3mgsa7- _2X_OUt_ _1WgeL1f _3VdCwhL _2Ryjovs"]/span/h1/a/@href').extract()
        for url in urls:
            yield scrapy.Request(url = 'https://www.seek.com.au/' + url, callback = self.parse_details, cb_kwargs = {'sal_range': sal_range})

        next_page = response.xpath('//*[@class="_24YOjgT"]/@href').extract_first()

        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, self.parse, cb_kwargs = {'index': index})

        time.sleep(2)

    def parse_details(self, response, sal_range):
        # Seek.com sneaks in promoted jobs in search results which are not counted towards total job found for the search
        if 'promoted' not in response.request.url:
            title = response.xpath('//*[@data-automation="job-detail-title"]/text()').extract_first()   
            company = response.xpath('//*[@data-automation="advertiser-name"]/text()').extract_first()
            
            location = response.xpath('//*[@class="FYwKg _3ftyQ _1lyEa"]/span[1]//*[@class="FYwKg PrHFr _1EtT-_4"]/text()').extract()
            if len(location) == 2:
                city = location[0]
                suburb = location[1]
            else:
                city = location[0]
                suburb = None

            category = response.xpath('//*[@class="FYwKg _3ftyQ _1lyEa"]/span[2]//*[@class="FYwKg PrHFr _1EtT-_4"]/text()').extract()
            if len(category) == 2:
                industry = category[0]
                department = category[1]
            else:
                industry = category[0]
                department = None

            work_type = response.xpath('//*[@data-automation="job-detail-work-type"]//text()').extract()
            if len(work_type) == 2:
                salary = work_type[0]
                employment_type = work_type[1]
            else:
                salary = None
                employment_type = work_type[0]

            content = [itm.replace(u'\xa0',u'') for itm in response.xpath('//*[@data-automation="jobAdDetails"]//text()').extract() if itm != ' ']
            text = ' '.join(content)

            yield {
                'job_id': re.findall('[0-9]+', response.request.url)[0],
                'title': title,
                'company': company,
                'city': city,
                'suburb': suburb,
                'industry': industry,
                'department': department,
                'salary': salary,
                'salary_label': sal_range,
                'employment_type': employment_type,
                'detail': text,
                'request_url': response.request.url
            }