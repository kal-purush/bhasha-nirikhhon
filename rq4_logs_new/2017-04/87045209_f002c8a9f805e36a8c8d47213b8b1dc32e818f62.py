import openpyxl
import xlrd
import xlwings as xw


filename = '/Users/ava/erpweb/static/Z_US_Clothing-R76005.xls'
wb = xlrd.open_workbook(filename)

# print(type(wb))

sheet = wb.sheet_by_name('Template')

rows = sheet.nrows
cols = sheet.ncols

a1 = sheet.cell(0, 0).value
b = sheet.cell_value(0, 0)

for row in range(0, rows):
    for col in range(0, cols):
        a = sheet.cell(row, col).value

app = xw.App(visible=True, add_book=False)
app.display_alerts = False
# app.screen_updating = False
filename2 = r'/Users/ava/erpweb/static/CA/AS1.xlsx'
wb = xw.books.open(filename2)
wb.sheets['sheet1'].range('A1').value = '苦短123123123123'

print(xw.App().version)