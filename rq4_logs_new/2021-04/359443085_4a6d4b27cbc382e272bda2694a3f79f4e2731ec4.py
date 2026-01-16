import gspread

gc = gspread.service_account(filename='./json_token.json')
sh = gc.open("Atlas-list")

mydata = sh.sheet1.get_all_records()
print(mydata)