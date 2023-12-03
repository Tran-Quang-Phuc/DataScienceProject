import scrapy
import requests
from bs4 import BeautifulSoup

class LinkedJobsSpider(scrapy.Spider):
    name = "linkedin"
    api_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?location=Biên%2BHòa%2C%2BDong%2BNai%2C%2BVietnam&geoId=103877324&trk=public_jobs_jobs-search-bar_search-submit&start='
    def start_requests(self):
        first_job_on_page = 0
        first_url = self.api_url + str(first_job_on_page)

        # headers = {
        #     'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
        # }

        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})


    def parse_job(self, response):
        first_job_on_page = response.meta['first_job_on_page']

        job_item = {}
        jobs = response.css("li")

        num_jobs_returned = len(jobs)
        print("******* Num Jobs Returned *******")
        print(num_jobs_returned)
        print('*****')
        
        for job in jobs:
            
            job_item['job_title'] = job.css("h3::text").get(default='not-found').strip()
            # job_item['job_detail_url'] = job.css(".base-card__full-link::attr(href)").get(default='not-found').strip()
            job_item['job_listed'] = job.css('time::text').get(default='not-found').strip()

            job_item['company_name'] = job.css('h4 a::text').get(default='not-found').strip()
            # job_item['company_link'] = job.css('h4 a::attr(href)').get(default='not-found')
            job_item['company_location'] = job.css('.job-search-card__location::text').get(default='not-found').strip()
                        
            dataid = job.css('div.base-card::attr(data-entity-urn)').get()
            if dataid :
                jobid = dataid.split(":")[3]

                print("------------------------------")
                print(jobid)
                print("------------------------------")

                target_url='https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{}'
                item_url = target_url.format(jobid)

                resp = requests.get(item_url)
                soup=BeautifulSoup(resp.text,'html.parser')

                items = soup.find("ul",{"class":"description__job-criteria-list"})

                if items:
                    items = items.find_all("li")
                    for item in items:
                        header = item.find('h3', {"class":"description__job-criteria-subheader"}).text.strip()
                        value = item.find('span', {"class":"description__job-criteria-text--criteria"}).text.strip()

                        job_item[header] = value

                

                yield job_item
        

        if num_jobs_returned > 0:
            first_job_on_page = int(first_job_on_page) + 25
            next_url = self.api_url + str(first_job_on_page)
            yield scrapy.Request(url=next_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})

    # def parse_job_details(self, response, item):
    #     level_text = response.css('description__job-criteria-list li::text').get()
