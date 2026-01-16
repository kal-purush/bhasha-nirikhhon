# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 02:07:00 2022

@author: micha
"""
import time
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains

import time
import json

browser = webdriver.Chrome("chromedriver.exe")

def login(browser, username, password): 
    browser.get("https://www.linkedin.com")
    username_key = browser.find_element(By.ID,"session_key")
    username_key.send_keys(username)
    password_key = browser.find_element(By.ID,"session_password")
    password_key.send_keys(password)
    login_button = browser.find_element(By.CLASS_NAME, "sign-in-form__submit-button")
    login_button.click()

def infiniteScroll(browser):
    # infinte scroll
    time.sleep(3)
    previous_height = browser.execute_script('return document.body.scrollHeight')
    
    while True:
        browser.execute_script('window.scrollTo(0, document.body.scrollHeight);')
        time.sleep(1)
        new_height = browser.execute_script('return document.body.scrollHeight')
        if new_height == previous_height:
            break
    
        previous_height = new_height
        
def getCompaniesJobLink(browser):
    
    # companies I follow on Linkedin
    browser.get("https://www.linkedin.com/in/michallevin25/details/interests/?detailScreenTabIndex=1")
    infiniteScroll(browser)
    
    # find all company links
    time.sleep(3)
    company_page = browser.find_elements(By.XPATH,"//a[contains(@href,'https://www.linkedin.com/company/')]")
    
    # create link for the job page for each company
    # TODO: company_page returns two times the same link. check if possible to remove one of them and then remove if statement
    companies_jobpages = []
    prev_page = company_page[0].get_attribute("href") + 'jobs/'
    for x in range (1,len(company_page)):
        jobs_page = company_page[x].get_attribute("href") + 'jobs/'
        if jobs_page != prev_page:
            companies_jobpages.append(jobs_page)
        prev_page = jobs_page
    
        
    return companies_jobpages

def no_jobs(curr_jobpage):
    browser.get(curr_jobpage)
    job_company = browser.find_elements(By.XPATH,'//h1')
    job_company = job_company[0].text
    print(job_company)
    job_info = None
    print('im here1')
    try:
        print('are there jobs?')
        # trying to look for jobs. if fails -> no jobs
        browser.find_element(By.XPATH,"//a[contains(@href,'/jobs/search/')]")
        print('yes there are jobs')        
    except:
        print('im here because there werent any jobs')
        data = {'Company name': [job_company],
                                 'Job Title': 'no jobs'}
        job_info = pd.DataFrame(data)
        print(job_info)
    finally:
        print('im hereeeee')
        return job_info

def yes_jobs(curr_jobpage):
    browser.get(curr_jobpage)
    time.sleep(3)
    all_jobs = browser.find_element(By.XPATH,"//a[contains(@href,'/jobs/search/')]")
       
    time.sleep(3)
    all_jobs.click()
    time.sleep(3)
    job_text = browser.find_elements(By.CLASS_NAME,"occludable-update")
    job_date = browser.find_elements(By.XPATH,"//time")
    job_company, job_titles, job_locations, job_post_times = [], [], [], []
    unwanted_text = 'See more jobs with these suggestions:'
    for i in range(len(job_text)):
        curr_job = job_text[i].text
        curr_date = job_date[i].get_attribute("datetime")
                # TODO: remove if statement and create scrolling 
        if len(curr_job) > 1:
            if unwanted_text not in curr_job:
                curr_job = curr_job.split('\n')
                job_titles.append(curr_job[0])
                job_company.append(curr_job[1])
                job_locations.append(curr_job[2])
                job_post_times = curr_date
                        
            
    data = {'Company name': job_company,
            'Job Title':job_titles,
            'Job Location': job_locations,
            'Date Posted': job_post_times}
    jobs_df = pd.DataFrame(data)
    print('before')
    print(jobs_df)
    job_info = jobs_df.loc[jobs_df['Job Location'].str.contains("Israel")]
    
    return job_info

def job_finder(curr_jobpage):
    job_info = no_jobs(curr_jobpage)
    print(job_info)
    print('job info in none if there are jobs')
    if job_info is None:
        
        print('im here because there are jobs')
        job_info = yes_jobs(curr_jobpage)
   
    return job_info


# a lot of jobs
curr_jobpage = 'https://www.linkedin.com/company/81904307/jobs/'

# no jobs
 #curr_jobpage = 'https://www.linkedin.com/company/aviv-scientific/jobs/'

# print(job_title, job_date)
#little jobs
curr_jobpage = 'https://www.linkedin.com/company/livemetric.com/jobs/'




# main
browser = webdriver.Chrome("chromedriver.exe")
# login(browser, "yourmail05@gmail.com","***" )
#companies_jobpages = getCompaniesJobLink(browser)
curr_jobpage = 'https://www.linkedin.com/company/vlx-ventures/jobs/'
job_info = job_finder(curr_jobpage)
print('after')
print(job_info)
"""
for curr_jobpage in companies_jobpages:
    job_info = job_finder(curr_jobpage)
    print('after')
    print(job_info)
"""


