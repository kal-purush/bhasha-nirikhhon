import xlsxwriter


def create_excel(filename, data):    
    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet()

    
    row = 0        
    for row_value in data:
        col = 0
        for cell_value in row_value:
            worksheet.write(row, col, cell_value)
            col += 1            
        row += 1

    workbook.close()