import os

a=1
while a==1:
	print('\n\n======== Prima Checker Fix ========')
	angka=int(input('\nMasukkan angka = '))
	prima=1

	if angka>1:
		for i in range(2,angka):
			if angka%i==0:
				print(angka,'Bukan Bilangan Prima')
				print('karena',i,'x',angka//i,'=',angka)
				prima=0
				#break
		else:
			if (prima==1):
				print(angka,'adalah bilangan prima')
	else:
		print(angka,'\nBukan Bilangan Prima')
	print('\n=== Copyright by @irfanfananim ===')
	input('\nPress Enter to Continue')
	os.system('cls')