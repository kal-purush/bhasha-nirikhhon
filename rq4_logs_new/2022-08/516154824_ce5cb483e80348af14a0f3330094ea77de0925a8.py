from rpi_lcd import LCD
import time

lcd = LCD()

while True:
    perintah = input('Enter command (show or clear): ')

    if perintah in ['show', 'Show']:
        line1 = input('Enter text for line 1: ')
        print(f'Line 1 has been set to show {line1}')

        line2 = input('Enter text for line 2: ')
        print(f'Line 2 has been set to show {line2}')

        lcd.text(line1,1)
        lcd.text(line2,2)

    if perintah in ['clear', 'Clear']:
        lcd.clear()

    else:
        print('Unknown command!')