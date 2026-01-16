from fpdf import FPDF
import pandas as pd

pdf = FPDF(orientation="P", unit="mm", format="A4")
pdf.set_auto_page_break(auto=False, margin=0)  #by default it is True so make it False

df = pd.read_csv("topics.csv")

for index, row in df.iterrows():    #Iterate through rows in "topics.csv" file
    pdf.add_page()  #adds a new page for every iteration,i.e, 39 times

    # Set the header
    pdf.set_font(family="Times", style="B", size=24)
    pdf.set_text_color(100, 100, 100)   #combination of r,g,b
    #254,0,0=red; 0,254,0=blue; 0,0,254-green
    pdf.cell(w=0, h=12, txt=row["Topic"], align="L",
         ln=1)  #page format
    for y in range(20, 298, 10):
        pdf.line(10, y, 200, y)  #starts from left margin around point 10 and ends around 200, 10 mm thickness

    # Set the footer
    pdf.ln(265) #sets footer around 265 so that it doesn't move out of the page
    pdf.set_font(family="Times", style="I", size=8)
    pdf.set_text_color(180, 180, 180)   #Gray
    pdf.cell(w=0, h=10, txt=row["Topic"], align="R")    #Bottom right aligned


    for i in range(row["Pages"] - 1):
        pdf.add_page()

        # Set the footer
        pdf.ln(277)
        pdf.set_font(family="Times", style="I", size=8)
        pdf.set_text_color(180, 180, 180)
        pdf.cell(w=0, h=10, txt=row["Topic"], align="R")

        for y in range(20, 298, 10):
            pdf.line(10, y, 200, y) #x from margin for starting line and ending line in a row vary but height(y)
            #from the line remains same
pdf.output("output.pdf")