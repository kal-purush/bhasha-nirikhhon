from pdf2image import convert_from_path
import PySimpleGUI as sg
import tempfile

f = tempfile.TemporaryFile(suffix='.png')
# d = tempfile.TemporaryDirectory()

# images = convert_from_path(
# 'C:\\Users\\Atsushi\\Downloads\\a\\2021gakusei_plan-01.pdf', 600)
# print(np.array(images[0])
# images[0].save('C:\\Users\\Atsushi\\Documents\\MapGenerator\\test.png', 'png')
# print(f.name)
# images[0].save(f, 'png')
# f.close()
# print(d.name)


def change_img(p):
    images = convert_from_path(str(p), 600)
    value = sg.popup_get_file("Save PNG File \'*.png\'.",
                              save_as=True,
                              file_types=(("Image File", ".png"), ))
    images[0].save(str(value), 'png')


def error_window(msg):
    error_layout = [[sg.Text(msg, key='error')],
                    [sg.Button('OK', key='ok', expand_x=True)]]
    sub_window = sg.Window('エラー',
                           error_layout,
                           modal=True,
                           keep_on_top=True,
                           auto_size_text=True)
    while True:
        sub_event, sub_value = sub_window.read()
        sub_window['error'].update(msg)
        if sub_event == sg.WIN_CLOSED:  #ウィンドウのXボタンを押したときの処理
            break
        if sub_event == 'ok':
            break
    sub_window.close()


sg.theme('Default')

main_layout = [[sg.Text('対象ファイルを指定してください．')], [sg.Text('ファイル(PDF)')],
               [
                   sg.InputText('', key='inputFilePath'),
                   sg.FileBrowse('ファイルを選択',
                                 target='inputFilePath',
                                 file_types=(('PDFファイル', '*.pdf'), ))
               ], [sg.Button('実行', key='go', expand_x=True)]]

main_window = sg.Window('PDF -> PNG',
                        main_layout,
                        resizable=True,
                        auto_size_buttons=True,
                        auto_size_text=True,
                        finalize=True)

main_window.set_min_size((470, 140))

while True:
    event, values = main_window.read()

    if event == sg.WIN_CLOSED:  #ウィンドウのXボタンを押したときの処理
        break

    if event == 'go':
        if not values['inputFilePath']:
            error_window('ファイル(PDF)が選択されていません．')
        else:
            change_img(values['inputFilePath'])
            break

main_window.close()