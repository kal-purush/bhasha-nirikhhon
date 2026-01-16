import mysql.connector
from fpdf import FPDF
connection = mysql.connector.connect(host="localhost",
                                             user="root",
                                             password="root",
                                             database="Smart_Society_db")

cursor = connection.cursor()
cursor.execute("select count(*) from member_register")
result = cursor.fetchall()
#print(result)
cursor = connection.cursor()
cursor.execute("select * from member_register")
result1 = cursor.fetchall()
#print(result1)

pdf=FPDF()
pdf.add_page()
pdf.set_font("Arial",size=8)
pdf.cell(200,50,txt="Smart_Society_Maintenance_Db_Member Report",ln=1,align="C")

for i in range(len(result)):
    k=35
    for j in range(len(result[i])):
        print(result[i][j])
        data=str(result[i][j])
        #pdf.cell(k,20,txt=data,align="C",border=1)

for i in range(len(result1)):
    k=20
    for j in range(len(result1[i])):
         print(result1[i][j])
         data1 = str(result1[i][j])

         #pdf.cell(k, 20, txt=data, align="C", border=1)

         pdf.cell(k, 15, txt=data1, align="C", border=1)

    pdf.ln()
pdf.output("TotalMem3.pdf")
