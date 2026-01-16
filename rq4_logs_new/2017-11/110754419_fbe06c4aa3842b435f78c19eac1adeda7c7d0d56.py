# Created by: Kay Lin
# Created on: 20-Nov-2017
# Created for: ICS3U
# This program shows calculate average of your mark

import ui

mark_list = []

def click_button_touch_up_inside(sender):
    # input your mark and show in the textview
    
    your_mark = int(view['mark_textfield'].text)
    
    if your_mark < 0 or your_mark > 100:
       view['result_label'].text = 'Please input valid number.'
       view['mark_textfield'].text = ''
    else:
       mark_list.append(your_mark)
       view['mark_textfield'].text = ''
       view['mark_textview'].text = 'Your marks: ' + str(mark_list)
        
def calculate_your_mark(mark_list):
    # calcualte the average of your mark
    
    average = int((sum(mark_list)/len(mark_list) + 0.5))
    
    return average
    
def average_button_touch_up_inside(sender):
    # show your average
    
    average_print = calculate_your_mark(mark_list)
    view['result_label'].text = 'Your average is ' + str(average_print) + '%'

view = ui.load_view()
view.present('full_screen')