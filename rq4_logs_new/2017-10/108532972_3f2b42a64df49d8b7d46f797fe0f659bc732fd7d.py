from subprocess import check_output
from bs4 import BeautifulSoup
from splinter import Browser
SENDMAIL = '/usr/bin/msmtp'
import os
import time

wait_delay = 4
with open('manuscript_present_status.txt', 'r') as myfile:
    previous_manuscript_status=myfile.read().replace('\n', '')

print 'Previous status of manuscript was : ' + previous_manuscript_status
time.sleep(wait_delay)

def get_pass():
    return check_output("gpg -dq ~/rsc_password.gpg", shell=True).strip("\n")

rsc_password_plaintext = get_pass()

b = Browser('chrome')
time.sleep(wait_delay)
b.visit('https://mc.manuscriptcentral.com/ee/')
time.sleep(wait_delay)
b.fill('USERID', 'krishnakumar@imperial.ac.uk')
time.sleep(wait_delay)
b.fill('PASSWORD',rsc_password_plaintext)
time.sleep(wait_delay)
b.click_link_by_id('logInButton')
time.sleep(wait_delay)
b.click_link_by_partial_href("AUTHOR")
time.sleep(wait_delay)
html_obj = b.html
soup = BeautifulSoup(html_obj,"lxml")
table = soup.find("table", attrs={"class":"table table-striped rt cf"})
row = table.tbody.findAll('tr')[1]
first_column_html = str(row.findAll('td')[1].contents[0])
current_manuscript_status = BeautifulSoup(first_column_html,"lxml").text
# current_manuscript_status = 'demo'
# print current_status_msg
time.sleep(wait_delay)
b.quit()

if current_manuscript_status == previous_manuscript_status:
    print 'Your manuscript status remains unchanged ....'
    print 'Please check back later. Bye.\n\n'
else:
    print "There has been a new status change.....Sending updated paper status through email ... "
    p = os.popen("%s -t" % SENDMAIL, "w")
    p.write("To: krishnak@vt.edu\n")
    p.write("Subject: Status Update: Layer Optimisation Manuscript RSC EES\n")
    p.write("\n") # blank line separating headers from body
    p.write("Current status of Manuscript is : " + current_manuscript_status + "\n")
    sts = p.close()
    if sts != 0:
        print "Sendmail exit status", sts
    text_file = open("manuscript_present_status.txt", "w")
    text_file.write(current_manuscript_status)
    text_file.close()