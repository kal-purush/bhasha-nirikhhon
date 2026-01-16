import xlrd
import csv
import sys,os

def csv_from_excel(baseName,xlsfile):
       #deal with numeric 
       cellNum =[0,11,12,13,14,15,16,17,19,22,23,24]
       wb = xlrd.open_workbook(xlsfile)
       isheet=0
       for sheetname in wb.sheet_names():
              sh = wb.sheet_by_name(sheetname)
              csvfile=open("{}{}.csv".format(baseName,isheet),'w',newline='',encoding='GB18030')
              wr =  csv.writer(csvfile,quoting=csv.QUOTE_NONE,quotechar='',escapechar='\\')
     # deal with mutilplines 
              iTotal=0
              for rownum in range(sh.nrows):
         
                     rowlist =[str(item).replace('\n',' ') for item in sh.row_values(rownum)]
                    
                     iTotal+=1
         
                     wr.writerow(rowlist)
              print("{}:Totla{}".format(sheetname,iTotal))
              csvfile.close()
              isheet+=1


def main():
       xlsfile = sys.argv[1]
       base= os.path.basename(xlsfile)
       basename=os.path.splitext(base)[0]       
       csv_from_excel(basename,xlsfile)


if __name__=='__main__':
       main()
     
