from matplotlib import pyplot
from openpyxl import load_workbook
wb=load_workbook('data_analysis_lab.xlsx')
sheet=wb['Data']
n=sheet['A'][1:0]

def getvalue(sheet): return sheet.value
map(getvalue, sheet['A'][1:])