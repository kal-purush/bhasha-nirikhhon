import os
import sys
import xlrd, xlwt
import time

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))

from order.parse_order import CheckOrders

ORDER_COLUMN_NUMBER = 12
STATUE_COLUMN_NUMBER = 19
TRACKING_COLUMN_NUMBER = 20
SHIP_DATE_COLUMN_NUMBER = 21

SHIPPING_STATUS_COLUMN_NUMBER = 22
DELIVER_DATE_COLUMN_NUMBER = 23


def update_oder_status(old_sheet, new_sheet, order_checker):
    for row in range(1, old_sheet.nrows):
        multiple_orders = old_sheet.cell_value(row, ORDER_COLUMN_NUMBER)
        valid_trackings = []
        multiple_trackings = []
        multiple_status = []
        multiple_ship_dates = []
        for order in multiple_orders.split('\n'):
            try:
                order_number, order_email = order.strip().split()
            except ValueError:
                continue
            # update order status
            status, tracking, ship_date = order_checker.check_one_order(order_number, order_email)
            multiple_trackings.append(tracking)
            multiple_status.append(status)
            multiple_ship_dates.append(ship_date)
            if tracking:
                valid_trackings.append(tracking)
        if multiple_trackings:
            new_sheet.write(row, TRACKING_COLUMN_NUMBER, '\n'.join(multiple_trackings))
            new_sheet.write(row, SHIP_DATE_COLUMN_NUMBER, '\n'.join(multiple_ship_dates))
            new_sheet.write(row, STATUE_COLUMN_NUMBER, '\n'.join(multiple_status))

        # update shipping status
        if valid_trackings:
            multiple_shipping_status = []
            multiple_deliver_dates = []
            tracking_dictionary = order_checker.check_shipping(valid_trackings)
            for tracking in valid_trackings:
                try:
                    shipping_status = tracking_dictionary[tracking]['status']
                    deliver_date = tracking_dictionary[tracking]['deliver_date']
                except KeyError:
                    shipping_status = deliver_date = ''
                if not deliver_date:
                    deliver_date = ''
                multiple_shipping_status.append(shipping_status)
                multiple_deliver_dates.append(deliver_date)
                new_sheet.write(row, SHIPPING_STATUS_COLUMN_NUMBER, '\n'.join(multiple_shipping_status))
                new_sheet.write(row, DELIVER_DATE_COLUMN_NUMBER, '\n'.join(multiple_deliver_dates))


def tracking_orders(file_path, new_sheet_name):
    new_sheet_name = new_sheet_name + time.strftime("%m%d%Y %H_%M_%S")

    old_book = xlrd.open_workbook(file_path)
    old_sheet = old_book.sheet_by_index(0)

    new_book = xlwt.Workbook()
    new_sheet = new_book.add_sheet(new_sheet_name, cell_overwrite_ok=True)

    #copy the ENTIRE old file to new file
    for col in range(old_sheet.ncols):
        for row in range(old_sheet.nrows):
            new_sheet.write(row, col, old_sheet.cell_value(row, col))

    #add the most updated status column
    new_sheet.write(0, old_sheet.ncols, 'status' + time.strftime("%m%d %H:%M"))

    order_checker = CheckOrders()
    update_oder_status(old_sheet, new_sheet, order_checker)

    directory, file_name = os.path.split(file_path)
    new_file_name = os.path.join(directory, new_sheet_name + '.xls')
    new_book.save(new_file_name)


if __name__ == '__main__':
    try:
        file_path = sys.argv[1]
    except IndexError:
        print 'please provide a excel file path and new sheet name.'
    else:
        try:
            new_sheet_name = sys.argv[2]
        except IndexError:
            new_sheet_name = ''
        tracking_orders(file_path, new_sheet_name)