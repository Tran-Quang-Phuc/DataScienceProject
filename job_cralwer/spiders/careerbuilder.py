import scrapy
import requests


class CareerbuilderSpider(scrapy.Spider):
    name = "careerbuilder"
    api_url = 'https://careerbuilder.vn/viec-lam/tat-ca-viec-lam-trang-{}-vi.html'
    
    def start_requests(self):
        initPage = 1;
        first_url = self.api_url.format(initPage)
        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'pageNum': initPage})


    def parse_job(self, response):

        jobs = response.css("div.job-item")
        
        job_item = {}
        for job in jobs :
            job_item['job_title'] = job.css("h2 a::text").get(default='not-found').strip()
            # job_item['job_detail_url'] = job.css(".base-card__full-link::attr(href)").get(default='not-found').strip()
            job_item['job_listed'] = job.css('div.time time::text').get(default='not-found').strip()

            job_item['company_name'] = job.css('a.company-name::text').get(default='not-found').strip()
        
            # job_item['company_link'] = job.css('h4 a::attr(href)').get(default='not-found')
            job_item['company_location'] = job.css('div.location li::text').get(default='not-found').strip()

            joblink = job.css('div.title h2 a.job_link::attr(href)').get()
            if joblink :
                yield scrapy.Request(url=joblink, callback=self.parse_job_detail, meta={'jobitem': job_item})

            yield job_item

        pageNum = response.meta['pageNum']
        pageNum = pageNum + 1
        next_page = self.api_url.format(pageNum)
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse_job,meta={'pageNum': pageNum})


    def parse_job_detail(self, response) :
        jobitem = response.meta['jobitem']
        jobDetail = response.css("div.bg-blue")
        
        jobitem['Seniority level'] = jobDetail.css('div.detail-box li:contains("Cấp bậc") p::text').get(default = 'not-found').strip()
        jobitem['Employment type'] = jobDetail.css('div.detail-box li:contains("Hình thức") p::text').get(default = 'not-found').strip()
        jobitem['Job function'] = "Other"
        industriesText = jobDetail.css('div.detail-box li:contains("Ngành nghề") p a::text').getall()
        jobitem['Industries'] = ', '.join([industry.strip() for industry in industriesText])

        yield jobitem