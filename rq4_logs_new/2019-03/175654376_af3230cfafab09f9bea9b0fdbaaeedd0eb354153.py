# 寫一個程式來一個讀取Excel檔，然後在cmd中印出想要的字串。
# (這個程式不需要寫入任何檔案，只需要讀取excel檔然後印東西到cmd上)
# 假設一個excel檔 A攔是人名，D攔是身份證字號，省略所有其他攔，我希望在CMD一行一行印出
# <人名>的身份證字號是<身份證字號>

import openpyxl
from openpyxl import Workbook
wb = openpyxl.load_workbook('input.xlsx')
ws = wb.active
ws.delete_rows(0)
for cell in ws['A']:
    name = cell.offset(column=0).value
    id_number = cell.offset(column=3).value
    print('%s的身份證字號是%s' % (name, id_number))