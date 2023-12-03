import scrapy
from scrapy.http import Request
from job_cralwer.items import VietnamworksItem


class Vietnamworks(scrapy.Spider):
    name = 'vietnamworks'
    MAX_PAGE = 5

    def start_requests(self):
        url = "https://www.vietnamworks.com/viec-lam?page=1"
        yield scrapy.Request(
            url=url,
            meta={"page_num": 1},
            callback = self.get_job_link
        )

    def get_job_link(self, response):
        page_num = response.meta["page_num"]
        if page_num > self.MAX_PAGE:
            return
        
        job_links = response.xpath("//div[@class='block-job-list']//h2/a/@href").getall()
        print(f"Number of job in page {page_num}: {len(job_links)}")
        for job_link in job_links:
            pass
            # yield scrapy.Request(url=job_link, callback=self.parse_job)
        
        next_page_url = response.url.replace(f"page={page_num}", f"page={page_num + 1}")
        yield scrapy.Request(
            url=next_page_url,
            meta={"page_num": page_num+1},
            callback=self.get_job_link
        )

    def parse_job(self, response):
        job_item = VietnamworksItem()
        yield job_item