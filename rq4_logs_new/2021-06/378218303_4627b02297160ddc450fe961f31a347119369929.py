# import xlsxwriter module
import xlsxwriter

# Workbook() takes one, non-optional, argument
# which is the filename that we want to create.
workbook = xlsxwriter.Workbook('bye.xlsx')

# The workbook object is then used to add new
# worksheet via the add_worksheet() method.
worksheet = workbook.add_worksheet()

# Use the worksheet object to write
# data via the write() method.
worksheet.write('A1', 'ID')
worksheet.write('B1', 'Name')
worksheet.write('C1', 'Semester')
worksheet.write('D1', 'Phone No')


# Use the worksheet object to write
# data via the write() method.
worksheet.write('A2', '053')
worksheet.write('B2', 'Maiza')
worksheet.write('C2', '7th')
worksheet.write('D2', '12345')


# Use the worksheet object to write
# data via the write() method.
worksheet.write('A3', '054')
worksheet.write('B3', 'Maiza')
worksheet.write('C3', '7th')
worksheet.write('D3', '12345')

# Use the worksheet object to write
# data via the write() method.
worksheet.write('A4', '055')
worksheet.write('B4', 'Maiza')
worksheet.write('C4', '7th')
worksheet.write('D4', '12345')

# Use the worksheet object to write
# data via the write() method.
worksheet.write('A5', '056')
worksheet.write('B5', 'Maiza')
worksheet.write('C5', '7th')
worksheet.write('D5', '12345')

# Use the worksheet object to write
# data via the write() method.
worksheet.write('A6', '057')
worksheet.write('B6', 'Maiza')
worksheet.write('C6', '7th')
worksheet.write('D6', '12345')

# Use the worksheet object to write
# data via the write() method.
worksheet.write('A7', '058')
worksheet.write('B7', 'Maiza')
worksheet.write('C7', '7th')
worksheet.write('D7', '12345')

# Use the worksheet object to write
# data via the write() method.
worksheet.write('A8', '058')
worksheet.write('B8', 'Maiza')
worksheet.write('C8', '7th')
worksheet.write('D8', '12345')

# Finally, close the Excel file
# via the close() method.
workbook.close()