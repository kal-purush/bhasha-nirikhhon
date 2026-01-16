#-------------------------------------------------------------------------------
# Name: Module1
# Purpose:
#
# Author:      yans
#
# Created:     06/15/2015
# Copyright:   (c) yans 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import xlrd
from xlrd import open_workbook
import xlutils
from xlutils.copy import copy
import xlwt
from xlwt import *

def diffColumnValues(columnValues1,columnValues2):
    columnNum = {}
    sameColumnNum = []
    diffColumnNum = []
    for i in range (1,len(columnValues1)):
       value1 = columnValues1[i]
       if value1 in columnValues2:
         sameColumnNum.append(i)
       else:
         diffColumnNum.append(i)
         #print 'The '+value1+' in the first sheet is not in the second sheet.'
    columnNum['same'] = sameColumnNum
    columnNum['diff'] = diffColumnNum
    return columnNum

def identifyRowIndexByBugID(columnValues2, bugID):
    for i in range (len(columnValues2)):
       if columnValues2[i] == bugID:
          return i

def diffSheet(sheet1,sheet2,columnNum,sheetNum):
    global wWorkBook1, wWorkBook2
    bugIDList1 = sheet1.col_values(columnNum)
    bugIDList2 = sheet2.col_values(columnNum)
    columnNumList1 = diffColumnValues(bugIDList1,bugIDList2)
    if len(columnNumList1['diff']):
        wSheet1 = wWorkBook1.get_sheet(sheetNum)
        for rowIndex1 in columnNumList1['diff']:
           print 'diff1:',rowIndex1
           #print sheet1_1.row_values(rowIndex1)
           wSheet1.row(rowIndex1).set_style(style0)
           for columnIndex1 in range(sheet1.ncols):
               wSheet1.write(rowIndex1, columnIndex1, sheet1.cell_value(rowIndex1, columnIndex1), style0)

    print '----------------------'
    columnNumList2 = diffColumnValues(bugIDList2,bugIDList1)
    if len(columnNumList2):
        wSheet2 = wWorkBook2.get_sheet(sheetNum)
        for rowIndex2 in columnNumList2['diff']:
           print 'diff2:',rowIndex2
           #print sheet2_1.row(rowIndex2)
           #print sheet2_1.row_values(rowIndex2)
           #wSheet2_1.row(rowIndex2).set_style(style1)
           for columnIndex2 in range(sheet2.ncols):
               wSheet2.write(rowIndex2, columnIndex2, sheet2.cell_value(rowIndex2, columnIndex2), style1)
    print '----------------------'
    if len(columnNumList1['same']):
       wSheet1 = wWorkBook1.get_sheet(sheetNum)
       for rowIndex3 in columnNumList1['same']:
           sameBugID = sheet1.cell_value(rowIndex3,columnNum)
           rowIndex4 = identifyRowIndexByBugID(bugIDList2,sameBugID)
           print 'same rowIndex3',rowIndex3
           print 'same rowIndex4',rowIndex4
           for columnIndex3 in range(sheet1.ncols):
               print columnIndex3
               if sheet1.cell_value(rowIndex3,columnIndex3) != sheet2.cell_value(rowIndex4,columnIndex3):
                  wSheet1.write(rowIndex3, columnIndex3, sheet1.cell_value(rowIndex3, columnIndex3), style2)


def main():
    global wWorkBook1, wWorkBook2,style0,style1,style2
#Open the Excel file to read data
    file1 = 'c:\Projects_for_Astro_WEM_Release_New.xls' #new file
    file2 = 'c:\Projects_for_Astro_WEM_Release_Old.xls' #old file
    workBook1 = open_workbook(file1)
    workBook2 = open_workbook(file2)

#Gets a work table
    #table = data.sheets()[0]          #Gets the index order
    #table = data.sheet_by_index(0) #Gets the index order
    sheet1_1 = workBook1.sheet_by_name('Projects')#By getting the name
    sheet1_2 = workBook1.sheet_by_name('User Stories')
    sheet1_3 = workBook1.sheet_by_name('Tasks')
    sheet2_1 = workBook2.sheet_by_name('Projects')
    sheet2_2 = workBook2.sheet_by_name('User Stories')
    sheet2_3 = workBook2.sheet_by_name('Tasks')
    wWorkBook1 = copy(workBook1)
    wWorkBook2 = copy(workBook2)

#Set cell color
#pattern0 is used for New Rows
    pattern0 = xlwt.Pattern() # Create the Pattern
    pattern0.pattern = xlwt.Pattern.SOLID_PATTERN # May be: NO_PATTERN, SOLID_PATTERN, or 0x00 through 0x12
    pattern0.pattern_fore_colour = 5 # May be: 8 through 63. 0 = Black, 1 = White, 2 = Red, 3 = Green, 4 = Blue, 5 = Yellow, 6 = Magenta, 7 = Cyan, 16 = Maroon, 17 = Dark Green, 18 = Dark Blue, 19 = Dark Yellow , almost brown), 20 = Dark Magenta, 21 = Teal, 22 = Light Gray, 23 = Dark Gray, the list goes on...
    style0 = xlwt.XFStyle() # Create the Pattern
    style0.pattern = pattern0 # Add Pattern to Style
#pattern2 is used for Delete Rows
    pattern1 = xlwt.Pattern() # Create the Pattern
    pattern1.pattern = xlwt.Pattern.SOLID_PATTERN # May be: NO_PATTERN, SOLID_PATTERN, or 0x00 through 0x12
    pattern1.pattern_fore_colour = 22 # May be: 8 through 63. 0 = Black, 1 = White, 2 = Red, 3 = Green, 4 = Blue, 5 = Yellow, 6 = Magenta, 7 = Cyan, 16 = Maroon, 17 = Dark Green, 18 = Dark Blue, 19 = Dark Yellow , almost brown), 20 = Dark Magenta, 21 = Teal, 22 = Light Gray, 23 = Dark Gray, the list goes on...
    style1 = xlwt.XFStyle() # Create the Pattern
    style1.pattern = pattern1 # Add Pattern to Style
#pattern3 is used for existing Rows but with diff
    pattern2 = xlwt.Pattern() # Create the Pattern
    pattern2.pattern = xlwt.Pattern.SOLID_PATTERN # May be: NO_PATTERN, SOLID_PATTERN, or 0x00 through 0x12
    pattern2.pattern_fore_colour = 2 # May be: 8 through 63. 0 = Black, 1 = White, 2 = Red, 3 = Green, 4 = Blue, 5 = Yellow, 6 = Magenta, 7 = Cyan, 16 = Maroon, 17 = Dark Green, 18 = Dark Blue, 19 = Dark Yellow , almost brown), 20 = Dark Magenta, 21 = Teal, 22 = Light Gray, 23 = Dark Gray, the list goes on...
    style2 = xlwt.XFStyle() # Create the Pattern
    style2.pattern = pattern2 # Add Pattern to Style

#Gets the entire rows and columns of values (returns)
    diffSheet(sheet1_1,sheet2_1,0,0) #param 3 indicates the column number that you want to used for diff. It starts with 0.
    diffSheet(sheet1_2,sheet2_2,1,1)
    diffSheet(sheet1_3,sheet2_3,1,2)

    wWorkBook1.save('c:\Projects_for_Astro_WEM_Release_New_diff.xls')
    wWorkBook2.save('c:\Projects_for_Astro_WEM_Release_Old_diff.xls')
    #print table.col_values(4)
#Gets the number of rows and columns
##    print sheet1_1.nrows
    print sheet1_1.ncols
##    print sheet2_1.nrows
    print sheet2_1.ncols
#Gets the cell



if __name__ == '__main__':

    main()