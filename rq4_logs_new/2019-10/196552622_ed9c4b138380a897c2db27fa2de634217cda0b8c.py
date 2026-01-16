#Mail Services
import smtplib
server=smtplib.SMTP("smtp.gmail.com",587)
server.starttls()
server.login("pankhuritrikha@gmail.com","***********")
message='''
Hello, this is Pankhuri from Chitkara University
'''
server.sendmail('pankhuritrikha@gmail.com','gupta7nitin@gmail.com',message)