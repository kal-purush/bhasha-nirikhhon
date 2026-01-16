# def in_box():
#     customer_name = str('Jasmine')
#     current_date = str('Novemeber 27, 2020')
#
# def inputes():
#     default_survey = '''{customer_name}, your feedback is realy important. Please give a positive feedback.
#     Have a great day ahead. Happy shopping. Today's date is {current_date}.'''
#
# def codes():
#     name_code = "{customer_name}"
#     date_code = "{current_date}"
#
# def change(codes, inputes, in_box):
#     if codes in inputes:
#         default_survey = default_survey.replace('{customer_name}', customer_name)
#         default_survey = default_survey.replace('{current_date}', current_date)
#         print(default_survey)
#
# change(codes, inputes, in_box)

# Note :
# str.replace(old, new [, count])

# REPLACE CODES HERE
customer_name_code = "{customer_name}"
date_code = "{current_date}"
product_name_code = "{product_name}"
product_color_code = "{product_color}"
product_type_code = "{product_type}"
product_size_code = "{product_size}"
product_weight_code = "{product_weight}"
product_price_code = "{product_price}"
repromised_date_code = "{repromised_date}"

# coddes ={
#     'customer_name_code' : "{customer_name}",
#     'date_code' : "{current_date}",
#     'product_name_code' : "{product_name}",
#     'product_color_code' : "{product_color}",
#     'product_type_code' : "{product_type}",
#     'product_size_code' : "{product_size}",
#     'product_weight_code' : "{product_weight}",
#     'product_price_code' : "{product_price}",
#     'repromised_date_code' : "{repromised_date}"
#
# }


# REPLACE FROM HERE
customer_name = str('Jasmine')
current_date = str('Novemeber 27, 2020')
product_name = 'Razor'
product_type = 'Shaver Machine'
product_price = str('Rs. 300')
product_color = 'Black'
repromise_date = 'December 1, 2020'


default_survey = '''{customer_name}, your feedback is realy important. Please give a positive feedback. Have a great day ahead. Happy shopping. Today's date is {current_date}.'''
default_survey_2 = '''{customer_name}, your feedback is realy important. Please give a positive feedback.
Have a great day ahead. Happy shopping. Today's date is {current_date}.'''
# https://stackoverflow.com/questions/30239092/how-to-get-multiline-input-from-user


# if customer_name_code in default_survey:
#     new = default_survey.replace('{customer_name}', customer_name)
#     new = new.replace('{current_date}', current_date)
# print(new)
# https://www.youtube.com/watch?v=kxPXS9uGpwQ&ab_channel=Harshitvashisth
count = 8
type_box = ''
for i in range(8):
    if i <= count:
        type_box = input('Type here: \n')
        inp = inp + type_box
        print(i)

user_typed_here = type_box
print('\n', user_typed_here)

# if customer_name_code in user_typed_here:
#     new = user_typed_here.replace('{customer_name}', customer_name)
#     new = new.replace('{current_date}', current_date)
#     print(new)
# else:
#     print(type_box)


# Example
# no_of_lines = 5
# lines = ""
# for i in xrange(no_of_lines):
#     lines+=input()+"\n"
#
# print(lines)