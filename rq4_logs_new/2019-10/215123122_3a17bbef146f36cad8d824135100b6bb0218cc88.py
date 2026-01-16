#! python3
# job_search_monster_jobs - Find jobs matching conditions on monster jobs and save them on a new folder.

from selenium import webdriver
from twilio.rest import Client
import requests, os, datetime, webbrowser, openpyxl, pyautogui, twilioTextMe


pyautogui.PAUSE = 0.5 # allow 5 seconds pause between pyautogui function calls.

# dowload first the webdriver for the browser to be used: geckodriver.ex for Firefox and chromedriver.exe for Chrome
browser = webdriver.Firefox() #create the webdriver object for Firefox.
#browser = webdriver.Chrome('C:\\Users\\keita\\AppData\\Local\\Programs\\Python\\Python37\\chromedriver.exe') #create the webdriver object for chrome.
browser.get('https://www.monster.com/jobs/advanced-search?intcid=skr_navigation_www_advanced-search')# open advanced search web page.

# find elements.
jobTitleInputElt = browser.find_element_by_id('txtKeyword')
jobLocationInputElt = browser.find_element_by_id('advLocation')
jobDateDropdownElt = browser.find_element_by_css_selector('#ctl00_ctl00_ctl00_body_body_wacCenterStage_ddlDate > option:nth-child(1)')# for any date
companyInputElt = browser.find_element_by_id('inptListCo0')
fullTimeCheckBoxElt = browser.find_element_by_id('rptJobTypes_checkbox_0')
contractCheckBoxElt = browser.find_element_by_id('rptJobTypes_checkbox_1')
partTimeCheckBoxElt = browser.find_element_by_id('rptJobTypes_checkbox_2')
internshipCheckBoxElt = browser.find_element_by_id('rptJobTypes_checkbox_3')
temporaryCheckBoxElt = browser.find_element_by_id('rptJobTypes_checkbox_4')
otherCheckBoxElt = browser.find_element_by_id('rptJobTypes_checkbox_5')
submitElt = browser.find_element_by_id('submitButton')

# load the xlsx file where the job and the number to send sms to is defined specifications are defined.
wb = openpyxl.load_workbook('jobSpecifications.xlsx')
ws = wb.active

# inputBoxFiller function definition. Type the value into the input box and enter the number of tab needed to give the focus to the next field.
def inputBoxFiller(value, numberOfTab):
    if value == None:
        pyautogui.typewrite(numberOfTab * '\t', 0.1) # 0.1 second of latence time.
    else:
        pyautogui.typewrite(value + numberOfTab * '\t', 0.1)

# give the focus to the first field (job title input box).
jobTitleInputElt.click()

# write the job title into the input element.
jobTitle = ws['D2'].value # get the job title from the xlsx file at the cellule C2.
inputBoxFiller(jobTitle, 1)
   
# write the company name into  the input element if it is provided.
company = ws['D3'].value
inputBoxFiller(company, 2)
        
# write the job location into the input element.
jobLocation = ws['D4'].value
inputBoxFiller(jobLocation, 1)

# checkBoxFiller function definition.
def checkBoxFiller(elt, value):
    if value.upper() == 'Y':
        elt.click()
    
# checking full time or not.
fullTime = ws['D5'].value
checkBoxFiller(fullTimeCheckBoxElt, fullTime)

# checking contract or not.
contract = ws['D6'].value
checkBoxFiller(contractCheckBoxElt, contract)
    
# checking part time or not.
partTime = ws['D7'].value
checkBoxFiller(partTimeCheckBoxElt, partTime)

# checking internship or not.
internship = ws['D8'].value
checkBoxFiller(internshipCheckBoxElt, internship)

# checking part temporary or not.
temporary = ws['D9'].value
checkBoxFiller(temporaryCheckBoxElt, temporary)
    
# checking other or not.
other = ws['D10'].value
checkBoxFiller(otherCheckBoxElt, other)

# choosing the job posting date.
jobDate = ws['D11'].value
jobDateDropdownElt = browser.find_element_by_css_selector('#ctl00_ctl00_ctl00_body_body_wacCenterStage_ddlDate > option:nth-child(%s)' %(jobDate))
postingDate = jobDateDropdownElt.text # save the posting date for later use.
jobDateDropdownElt.click()

print('Finding jobs...')
submitElt.click()# submit the form
browser.refresh()# refresh the browser to let it load all elements.

# click the load more elt button to display all jobs.
while True:
    try:
        loadMoreJobElt = browser.find_element_by_id('loadMoreJobs') # button that is clicked to expand job list.
        loadMoreJobElt.is_displayed() # return True or an exception. An exception is returned when the job list is all expanded (no more load button is displayed.) 
        loadMoreJobElt.click() # if the button is displayed, it is clicked and more job are loaded.
    except:
        jobListElts = browser.find_elements_by_css_selector('div.summary > header > h2 > a')# list of all elts matching the css selector.
        print('%s jobs links have been found for this search.\nDownloading...' %(len(jobListElts)))
        break

# download jobs in html format and save them into a new folder.
newFolder = 'Jobs_%s_for_%s' %(datetime.date.today().strftime("%a_%b_%d"), postingDate)
os.makedirs(newFolder, exist_ok=True)

jobFound = 0
for elt in jobListElts: 
    print('Downloading job_%s...' %(jobListElts.index(elt) + 1))
    try:
        res = requests.get(elt.get_attribute('href'))
        res.raise_for_status()
    except:
        print("Couldn't dowmload this job")
        continue
    fileObj = open(os.path.join(newFolder, 'job_%s.html' %(jobListElts.index(elt) + 1)), 'wb')
    
    for chunk in res.iter_content(10000):
        fileObj.write(chunk)
    fileObj.close()
    jobFound += 1
print('%s jobs have been found and dowloaded for this search in the folder %s' %(jobFound, newFolder))

# sending messages to the job seeker's phone.
def textMe(message):
    # Preset values. Replace them by your own twilio account information.
    accountSID = 'yourTwilioAccountID'
    authToken = 'yourTwilioAuthToken'
    fromNumber = 'yourTwilioNumber'
    
    toNumber = ws['D18'].value
    
    client = Client(accountSID, authToken)
    client.messages.create(body=message, from_=fromNumber, to=toNumber)
    
jobUrlList = []
for elt in jobListElts:
    jobUrlList.append(elt.get_attribute('href')) # list of jobs' url.

message = "%s jobs, posted for %s since %s in %s have been found.\nCheck at %s on your computer for jobs' description." \
           %(len(jobUrlList), jobTitle, postingDate, jobLocation, os.path.abspath(newFolder)) 
textMe(message)

# send a message containing up to 5 jobs description links among all the jobs found.
maxJob = 5
if len(jobUrlList) <= maxJob:
    maxJob = len(jobUrlList)
    
if len(jobUrlList) > maxJob:
    message = "Check the %s first job(s)' description:\n" %(maxJob)
    links = ''
    for i in range(maxJob):
        links += jobUrlList[i] + '\n'
    message += links
else:
    message = "Check the %s job(s)' description:\n" %(maxJob)
    links = ''
    for i in range(maxJob):
        links += jobUrlList[i] + '\n'
    message += links
textMe(message)
