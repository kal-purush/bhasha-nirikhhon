from bs4 import BeautifulSoup
import requests
from matplotlib import pyplot as plt

filename = "college.csv"
f = open(filename, 'w')
headers = 'Institute_Name, Address, District, Contact, STD_code, Email_id, Website, Principal_name, Registrar_name'
f.write(headers)
j = 3007
mumbai_city=0
mumbai_sub=0
thane=0
raigad=0
pune=0
for i in range(0, 1000):
    url = 'http://dtemaharashtra.gov.in/frmInstituteSummary.aspx?InstituteCode=' + str(j)
    source = requests.get(url).text

    soup = BeautifulSoup(source, 'html.parser')

    containers = soup.find_all('table', {'class': 'AppFormTable'})

    for container in containers:
        name = container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblInstituteNameEnglish'})
        if "Polytechnic" in name[0].text or "Technical" in name[0].text or "Engineering" in name[0].text or "Technology" in name[0].text:
            print("Institute name: "+name[0].text)
            ins_name = name[0].text

            address = container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblAddressEnglish'})
            print("Address: "+address[0].text.replace('\n', ' '))
            ins_address = address[0].text

            district = container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblDistrict'})
            print("District: "+district[0].text)
            if "Mumbai City" in district[0].text:
                mumbai_city += 1
            elif "Mumbai Suburban" in district[0].text:
                mumbai_sub += 1
            elif "Raigad" in district[0].text:
                raigad += 1
            elif "Thane" in district[0].text:
                thane += 1
            elif "Pune" in district[0].text:
                pune += 1

            contact = container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblOfficePhoneNo'})
            print("Contact: "+contact[0].text)

            std_code = container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblSTDCode'})
            print("Std code: " + std_code[0].text)

            email = container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblEMailAddress'})
            print("Email: "+email[0].text)

            website = container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblWebAddress'})
            print("Website: "+website[0].text)

            principal= container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblPrincipalNameEnglish'})
            print("Principal name: "+ principal[0].text)

            registrar = container.find_all('span', {'id': 'ctl00_ContentPlaceHolder1_lblRegistrarNameEnglish'})
            print("Registrar name: "+ registrar[0].text)
            print("----------------------------------")
            if len(contact[0].text)>7:
                f.write("\n" + ins_name.replace(",", "|").replace('\n', ' ') + "," + ins_address.replace(",", "|").replace('\n', ' ') + "," + district[0].text + "," + contact[0].text+","+std_code[0].text+',' + email[0].text.replace(",", "|") + "," + website[0].text+','+principal[0].text+','+ registrar[0].text)
    j += 1
    if j == 3509:
        j = 6004

region_x = ["Mumbai City","Mumbai Suburban", "Thane", "Raigad", "Pune"]
numberOfColleges_y = [mumbai_city, mumbai_sub, thane, raigad, pune]
plt.axis('equal')
plt.title('Percentage of colleges district-wise under Mumbai & Pune region')
plt.pie(numberOfColleges_y, labels=region_x, radius=1, autopct='%0.2f%%', shadow=True, explode=[0.1, 0, 0, 0, 0])
plt.show()
f.close()