import smtplib #library for email protos

tasnia = 'tasnia1716@gmail.com'
aqib = 'aqibchowdhuryac@gmail.com'
riley = 'rileyvelasco98@gmail.com'
ian = 'iansrinehart@yahoo.com'
bryce = 'brycebell87ba@gmail.com'
mir = 'keytest42@gmail.com'

conn = smtplib.SMTP('smtp.gmail.com' , 587) #connection object
#print(conn) checking if functional

startConnection = conn.ehlo() #start the connection
#print(startConnection) starting with 200 means that we are good

startTls = conn.starttls()
#print(startTls)

loginEmail = conn.login('keytest42@gmail.com' , 'testkey42')
#print(loginEmail) works
#startCounter = 0
#endCounter = 5
for send in range(0,5):
    conn.sendmail('keytest42@gmail.com', mir,
              'Subject : going to hack you\n\n mircool\n\n')
conn.quit()