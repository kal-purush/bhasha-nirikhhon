import csv, os
import PySimpleGUI as sg
from pathlib import Path
import pandas as pd

DICT_3G = {
    'UBBPe3': 12,
    'UBBPe4': 12,
    'UBBPd1': 6,
    'UBBPd5': 6,
    'UBBPe6': 12,
    'UBBPg1a': 12,
}

DICT_4G = {
    'UBBPe3': 6,
    'UBBPe4': 6,
    'UBBPd1': 3,
    'UBBPd5': 3,
    'UBBPe6': 12,
    'UBBPg1a': 6,
    'UBBPg2a': 12
}

class CSVReader():
    def __init__(self, csv_file_r: csv.DictReader, site: str, cell_3g: int, cell_4g: int, cell_5g: int) -> None:
        self.csv_file_r = csv_file_r
        self.site_info: dict = {}
        self.site = self.check_site_existence(site)
        self.new_site_info = {'Site Name': self.site, 
                              1: '', 
                              2: 'UBBPg1a', 
                              3: '', 
                              4: '', 
                              5: ''}
        self.cell_3g = cell_3g
        self.cell_4g = cell_4g
        self.cell_5g = cell_5g
        

    def check_site_existence(self, site):
        for row in self.csv_file_r:
            if row['Site Name'] == site:
                self.site_info['Site Name'] = site

                for x in range(0, 6):
                    self.site_info[x] = row[str(x)]

                return site

        return None


    def deal_with_3G(self):
        for key, value in self.site_info.items():
            if value == 'UBBPe3' or value == 'UBBPe4':
                self.new_site_info[0] = value
                self.site_info[key] = ''
                break
            
        if not self.new_site_info[0]:
            for key, value in self.site_info.items():
                if value == 'UBBPg1a':
                    self.new_site_info[0] = value
                    self.site_info[key] = ''
                    break
        
        if not self.new_site_info[0]:
            self.new_site_info[0] = 'UBBPg1a'

        if DICT_3G[self.new_site_info[0]] < self.cell_3g:
            for key, value in self.site_info.items():
                if value == 'UBBPe3' or value == 'UBBPe4':
                    self.new_site_info[5] = value
                    self.site_info[key] = ''
                    break
            
            if not self.new_site_info[5]:
                for key, value in self.site_info.items():
                    if value == 'UBBPd1':
                        self.new_site_info[5] = value
                        self.site_info[key] = ''
                        break
        
            if not self.new_site_info[5]:
                self.new_site_info[5] = 'UBBPd1'


    def deal_with_4G(self):
        for key, value in self.site_info.items():
            if value == 'UBBPe3' or value == 'UBBPe4' or value == 'UBBPe6':
                self.new_site_info[1] = value
                self.site_info[key] = ''
                break
        
        if not self.new_site_info[1]:
            for key, value in self.site_info.items():
                if value == 'UBBPg2a':
                    self.new_site_info[1] = value
                    self.site_info[key] = ''
                    break
        
        if not self.new_site_info[1]:
            self.new_site_info[1] = 'UBBPg2a'
        
        if DICT_4G[self.new_site_info[1]] < self.cell_4g:
            for key, value in self.site_info.items():
                if value == 'UBBPe3' or value == 'UBBPe4' or value == 'UBBPe6':
                    self.new_site_info[3] = value
                    self.site_info[key] = ''
                    break
            
            if not self.new_site_info[3]:
                for key, value in self.site_info.items():
                    if value == 'UBBPg2a':
                        self.new_site_info[3] = value
                        self.site_info[key] = ''
                        break
        
            if not self.new_site_info[3]:
                self.new_site_info[3] = 'UBBPg2a'

        if self.new_site_info[3] and DICT_4G[self.new_site_info[1]] + DICT_4G[self.new_site_info[3]] < self.cell_4g:
            if not self.new_site_info[5]:
                for key, value in self.site_info.items():
                    if value == 'UBBPe3' or value == 'UBBPe4' or value == 'UBBPe6':
                        self.new_site_info[5] = value
                        self.site_info[key] = ''
                        break
                
                if not self.new_site_info[5]:
                    for key, value in self.site_info.items():
                        if value == 'UBBPg2a':
                            self.new_site_info[5] = value
                            self.site_info[key] = ''
                            break
            
                if not self.new_site_info[5]:
                    self.new_site_info[5] = 'UBBPg2a'
    
    def deal_with_04_slot(self):
        if self.cell_5g:
            self.new_site_info[4] = 'UBBPg3a'
                

def main():
    sg.theme("SystemDefault")
    
    USERNAME = "user_rnp"
    PASSWORD = "2023"
    
    login_layout = [
        [sg.Text('\nПрограмма по расположения плат', key='-TEXT-', justification='center', font=('Arial bold', 14))],
        [sg.Text("")],
        [sg.Text("Имя пользователя:",  size=(15)), sg.Input(key="-USERNAME-", size=(20))],
        [sg.Text("Пароль:", size=(15)), sg.Input(key="-PASSWORD-", size=(20), password_char="*")],
        [sg.Column([[sg.Button('Войти', key="-SIGN_IN-")]], justification='r')]
    ]

    login_window = sg.Window("Вход RNP", login_layout, auto_size_buttons=False, element_justification="center", font=("Arial", 12), finalize=True)

    while True:
        login_event, login_values = login_window.read()
        if login_event == sg.WIN_CLOSED:
            login_window.close()
            break

        elif login_event == "-SIGN_IN-":
            if login_values["-USERNAME-"] == USERNAME and login_values["-PASSWORD-"] == PASSWORD:
                login_window.close()
                break
            else:
                sg.popup_error("Неверное имя пользователя или пароль. Пожалуйста, повторите попытку.", title="Ошибка")
    
    information = "Вот краткий обзор функциональности скрипта:\n \
                \nСкрипт запрашивает (выбрать файл, ввести название сайта, ввести селлы) \
                \nПосле нажатья на кнопку 'Обработать данные' скрипт - обработает прогонит по функциям и создаст новый файл с сайтом и теми платами платами"
             
    working_directory = os.getcwd()
    headings_ubb = ["Site Name", "00, 02, 04, 06", "01, 03, 05, 07"]
    headings = list(headings_ubb)
    
    file_list_column = [
        [sg.Text("Выберите CSV файл для обработки и вывода в таблицу:")],
        [sg.In(size=(40, 1), enable_events=True, key="-FILE_PATH-"),
        sg.FileBrowse(button_text = "Выбрать файл", initial_folder=working_directory, file_types=[("CSV Files", "*.csv"), ("ALL Files", "*.*"),])],
        [sg.Text("Введите название сайта:")],
        [sg.In(size=(20, 1), key="-SITE_NAME-")],
        [sg.Text("Количество селлов для 3G:"), sg.In(size=(20, 1), pad=(10, 5), enable_events=True, key="-CELL_3G-")],
        [sg.Text("Количество селлов для 4G:"), sg.In(size=(20, 1), pad=(10, 5), enable_events=True, key="-CELL_4G-")],
        [sg.Text("Количество селлов для 5G:"), sg.In(size=(20, 1), pad=(10, 5), enable_events=True, key="-CELL_5G-")],
    ]
    
    table_list_column = [
        [sg.Table(values=[], justification="center", enable_events=True, visible=False, headings=headings_ubb, vertical_scroll_only = True, hide_vertical_scroll=True, auto_size_columns=True, expand_x=True,  max_col_width=100, key="-TABLE-")]]
    
    layout = [
        [sg.Text('Для отдела RNP обработчик по расположения плат', enable_events=True, key='-TEXT-', pad=(15, 0), font=('Any', 16))],
        [sg.Frame("1. Загрузка Файла CSV", file_list_column, size=(510, 250), title_color="blue", pad=(15, 20))],
        # sg.VSeperator(),
        [sg.Frame("2. Вывод в таблицу", table_list_column, size=(510, 180), title_color="blue", pad=(15, 20))],
        [sg.Text("", size=(17, 1)), sg.Button("Обработать данные", key="-UPDATE_FILE-"),  sg.Button("Информация", key="-INFO-"), sg.Exit("Выйти", key="-EXIT_APP-")]
    ]

    window = sg.Window("RNP Обработчик по расположения плат", layout, finalize=True, font="Arial 12", element_justification="center", auto_size_buttons=True)
    
    if login_values["-USERNAME-"] == USERNAME and login_values["-PASSWORD-"] == PASSWORD:
        while True:
            event, values = window.read()
            if event in (sg.WIN_CLOSED, "-EXIT_APP-"):
                break
            
            elif event == "-INFO-":
                sg.popup_ok(information, sg.version, font="Arial 12", grab_anywhere=True)
                
            elif event == "-UPDATE_FILE-":
                file_name = values["-FILE_PATH-"]
                site_name = values["-SITE_NAME-"]
                if not file_name:
                    sg.popup_error("Ошибка: Выберите файл CSV.", title="Ошибка")
                    
                elif not site_name:
                    sg.popup_error("Ошибка: Введите название сайта.", title="Ошибка")
                else:
                    cell_3g = int(values["-CELL_3G-"]) if values["-CELL_3G-"] else 0
                    cell_4g = int(values["-CELL_4G-"]) if values["-CELL_4G-"] else 0
                    cell_5g = int(values["-CELL_5G-"]) if values["-CELL_5G-"] else 0

                    if not (cell_3g or cell_4g or cell_5g):
                        sg.popup_error("Ошибка: Введите количество селлов для 3G, 4G, 5G.", title="Ошибка")
                    else:
                        with open(file_name, mode='r', newline='', encoding='utf-8-sig') as csvfile:
                            csv_file_reader = csv.DictReader(csvfile, delimiter=';')

                            csv_reader = CSVReader(csv_file_reader, site_name, cell_3g, cell_4g, cell_5g)

                            if not csv_reader.site:
                                sg.popup_error('Такого сайта нет')
                            
                            if cell_3g != 0:
                                csv_reader.deal_with_3G()
                            
                            if cell_4g != 0:
                                csv_reader.deal_with_4G()
                            
                            if cell_5g != 0:
                                csv_reader.deal_with_04_slot()

                            # Получить значения для столбцов 00 и 01
                            keys_00 = [0, 2, 4]
                            keys_01 = [1, 3, 5]

                            # Create a list of dictionaries for the data to write
                            data_to_write = []
                            for i in range(len(keys_00)):
                                if i == 0:
                                    site_name_value = csv_reader.site
                                else:
                                    site_name_value = ''
                                
                                data_to_write.append({
                                    'Site Name': site_name_value,
                                    '00, 02, 04, 06': csv_reader.new_site_info[keys_00[i]],
                                    '01, 03, 05, 07': csv_reader.new_site_info[keys_01[i]]
                                })

                            new_file_name = f'{csv_reader.site}.csv'
                            with open(new_file_name, mode='w', newline='', encoding='utf-8-sig') as csvfile:
                                fieldnames = headings_ubb
                                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
                                writer.writeheader()

                                for row in data_to_write:
                                    writer.writerow(row)
                            
                            if not new_file_name:
                                sg.popup_error("Файл еще не обработан. Пожалуйста, обработайте данные сначала.")
                            else:
                                df = pd.read_csv(new_file_name, delimiter=';')
                                df.replace(pd.NA, '', inplace=True)
                                table_data = [df.columns.tolist()] + df.values.tolist()
                                data_without_header = table_data[1:]
                                window["-TABLE-"].update(values=data_without_header, visible=True)
                                
                            # sg.popup_ok(f'Файл - {new_file_name} успешно обработан.')

    window.close()
if __name__ == '__main__':
    main()