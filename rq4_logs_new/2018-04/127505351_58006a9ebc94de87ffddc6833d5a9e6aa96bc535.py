#!usr/bin/env python3
#simplcalc0.py

import os
from functools import partial

def askme(question,type,errmsg):
	while True:
		user_input = input(question)
		if user_input != '':
			try:
				user_input == type(user_input)
			except ValueError:
				print(f'{user_input} is insufficient. Please read:')
				print(errmsg)
			else:
				print(f'{user_input} is acceptable.')
				return user_input
		else:
			continue

def calculator():
	while True:
		print(f'you are here: {os.getcwd()}')
		
		dirch = partial(askme,type=str,errmsg='\'s\'to stay in this directory\ntype new dir address to change')
		dirname=dirch('enter dir? ')
		if dirname != 's':
			os.chdir(dirname)
			print(os.getcwd())
		
		while True:
			print('''your options:\n
			1.  add
			2.  subtract
			3.  multiply
			4.  divide
			''')
			
			num_list = []
			
			menu_selection = partial(askme,type=int,errmsg='please enter NUMBER choice. Thank you.')
			menu = menu_selection('please choose from above menu. >>  ')
			if menu > 4 or menu <= 0:
				print('please read menu carefully...')
				continue
		
		list_make = partial(askme, type=int, errmsg='must be whole numbers.')
		while True:
			populate=list_make('enter a whole number...')
			num_list.append(populate)
			
			keep_going=partial(askme, type=str, errmsg='must be either \'y\' or \'n\'.')
			keep_populating =  keep going('want to keep inserting data (aka numbers)?')
			if keep_populating != 'y':
				break
		
		if menu == 1:
			operation = reduce(lambda a,b: a+b,num_list)
		elif menu == 2:
			operation = reduce(lambda a,b: a-b, num_list)
		elif menu == 3:
			operation = reduce(lambda a,b: a*b, num_list)
		elif menu == 4:
			operation = reduce(lambda a,b: a/b, num_list)
		
		
		
		file_name = partial(askme, type=str, errmsg='please remember extension...')
		filen = file_name('please enter a filename with extension >> .txt is a good choice.') 
		with open(filen, 'a+') as f:
			f.write('your values are:')
			for k,v in enumerate(num_list):
				f.write(k,'.  ',v)
			f.write(f'your result is: {operation}')
		
		print('thank you....')
		
		print('care to continue with a new data set?')
		choice = partial(askme,type=str,errmsg='please choose [y/n]')
		choice_input = choice('continue or leave? [y/n]')
		if choice_input == 'y' or choice_input == 'yes':
			continue
		else:
			break

if __name__=='__main__':
	calculator()
else:
	print('sorry no can do...')