import FreeSimpleGUI as sg
import datetime as dt
import xlsxwriter as xw
import req
import os

# list of calendars in Booked4Us. The index corresponds to the ID.
CALENDARS = ['Csoportos edzések', 'Kezelések']

def get_last_month_start():
    today = dt.date.today()
    first_day_this_month = today.replace(day=1)
    last_month_last_day = first_day_this_month - dt.timedelta(days=1)
    first_day_last_month = last_month_last_day.replace(day=1)
    return first_day_last_month

def get_last_month_end():
    today = dt.date.today()
    first_day_this_month = today.replace(day=1)
    last_month_last_day = first_day_this_month - dt.timedelta(days=1)
    return last_month_last_day

def create_window():
    label_calendar = sg.Text('Naptár', size=17)
    input_calendar = sg.DropDown(CALENDARS, default_value='Csoportos edzések', key='calendar')

    last_month_start = get_last_month_start()
    last_month_end = get_last_month_end()
    label_from = sg.Text('Ettől a dátumtól', size=17)
    input_from = sg.Input(key='date_from', default_text=last_month_start.strftime("%Y-%m-%d"))
    button_from = sg.CalendarButton('Naptár', 'date_from', default_date_m_d_y=(last_month_start.month, last_month_start.day, last_month_start.year), format='%Y-%m-%d', size=(10, 0))

    label_to = sg.Text('Eddig a dátumig', size=17)
    input_to = sg.Input(key='date_to', default_text=last_month_end.strftime("%Y-%m-%d"))
    button_to = sg.CalendarButton('Naptár', 'date_to', default_date_m_d_y=(last_month_end.month, last_month_end.day, last_month_end.year), format='%Y-%m-%d', size=(10, 0))

    label_export_dest = sg.Text('Excel file helye', size=17)
    input_export_dest = sg.Input(key='excel_dest')
    button_export_dest = sg.FolderBrowse('Kiválaszt', key="export_dest", size=(10, 0))

    button_get = sg.Button("Lekérdez", key='query', size=8)
    button_exit = sg.Button("Kilép", key='exit', size=8)

    title = 'Foglalások lekérdezése'
    main_window = sg.Window(title,
                       layout = [
                           [label_calendar, input_calendar],
                           [label_from, input_from, button_from],
                           [label_to, input_to, button_to],
                           [label_export_dest, input_export_dest, button_export_dest],
                           [button_get, button_exit]
                       ])
    return main_window

def response_to_xlsx(reservations_response, filepath):
    workbook = xw.Workbook(filepath)
    worksheet = workbook.add_worksheet()
    # add header
    worksheet.write_row(0, 0, ['Időpont', 'Típus', 'Vendég', 'Vendégek száma'], cell_format=workbook.add_format({'bold': True}))

    for idx, booking in enumerate(reservations_response):
        # convert date from ISO to something more pleasant to the eye
        start_time = dt.datetime.strptime(booking['StartTime'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M")
        worksheet.write_row(idx+1, 0, [start_time, booking['Title'], booking['User']['Name']])
        worksheet.write_formula(idx+1, 3, f'COUNTIF(A:A, A{idx+2})')
    worksheet.autofilter(f'A1:C{len(reservations_response) + 1}')
    worksheet.autofit()
    workbook.close()

def create_popup(filepath):
    popup_title = 'Az Excel file elkészült'
    button_open = sg.Button("Megnyit", key='open', size=8)
    button_close_popup = sg.Button("Bezár", key='close', size=8)

    popup_event, _ = sg.Window(popup_title,
                               layout=[[sg.T(filepath)],
                                       [button_open, button_close_popup]],
                               disable_close=True).read(close=True)
    if popup_event == 'open':
        os.system('start ' + filepath)


# get token at startup
token = req.get_token()

window = create_window()

while True:
    event, values = window.read()
    if event == 'exit' or event == sg.WIN_CLOSED:
        break
    if event == 'query':
        # the request expects the ISO format
        date_from = values['date_from'] + 'T00:00:00.000Z'
        date_to = values['date_to'] + 'T00:00:00.000Z'

        # create request
        calendar_id = CALENDARS.index(values['calendar']) + 1
        response = req.get_reservations(date_from, date_to, calendar_id, token)
        reservations = response['Data']['Reservations']

        # create XLSX file
        xls_filepath = f"{values['export_dest']}/eletmodfitnesz_{calendar_id}_{values['date_from']}_{values['date_to']}.xlsx"
        response_to_xlsx(reservations, xls_filepath)

        # create popup when ready
        create_popup(xls_filepath)


window.close()