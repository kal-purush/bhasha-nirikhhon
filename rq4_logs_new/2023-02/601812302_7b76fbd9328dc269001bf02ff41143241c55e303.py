from asyncore import read
import sys, os
def add_product():
    with open('store.txt', 'a') as file:
          c = 'y'
          while c == 'y':
               name = input('Enter product name : ')
               if exist(name) : print('This product already Exist !')
               else :
                    price = input('Enter product price : ')
                    file.write(name +'\t'+ price +'\n')
               c = input('Do you want to add again (y / n) : ')
               print('-------------------------------')
          print("Product saved successfully !\n-------------------------------")


def add_exist_product(name):
     print('This product already purchase !')
     file = open('reset.txt', 'r')
     tmpfile = open('tmp_reset.txt', 'w')
     for l in file:
          field = l.split('\t')
          if field[0] == name :
               quantity = int(input('The quantity you want to add to '+field[0]+' : '))
               quantity = int(field[1])+quantity
               price = quantity*get_price(name)
               l = name +'\t'+ str(quantity) +'\t'+ str(price) +'\n'
          tmpfile.write(l)
     file.close()
     tmpfile.close()
     os.remove('reset.txt')
     os.rename('tmp_reset.txt', 'reset.txt')
     print("Product purchased successfully !\n-------------------------------")

     


def show_store():
     with open('store.txt', 'r') as file:
          print('Name\tprice\n-------------------------------')
          for l in file: print(l, end = '')
     print('-------------------------------')


def search_product():
     name = input("Enter Product Name to search : ")
     with open('store.txt', 'r') as file:
          flag = True
          for l in file:
               field = l.split('\t')
               if field[0] == name :
                    print('Found !\nName\tPrice\n-------------------------------')
                    print(l, end = '')
                    flag = False    
          if flag : print('This Product Not in the store\n-------------------------------')

def exist(name):
     with open('store.txt', 'r') as file:
          for l in file:
               field = l.split('\t')
               if field[0] == name : return True    
          return False

def exist_in_reset(name):
     with open('reset.txt', 'r') as file:
          for l in file:
               field = l.split('\t')
               if field[0] == name : return True    
          return False


def remove_product():
     name = input("Enter Product Name to Remove : ")
     file = open('store.txt', 'r')
     tmpfile = open('tmp_store.txt', 'w')
     flag = True
     for l in file:
          field = l.split('\t')
          if field[0] == name :
               flag = False
          else :
               tmpfile.write(l)
     file.close()
     tmpfile.close()
     os.remove('store.txt')
     os.rename('tmp_store.txt', 'store.txt')
     if flag : print('This Product Not Found\n-------------------------------')
     else: print('Product Removed Successfully\n-------------------------------')

def update_product():
     name = input("Enter Product Name to Edit : ")
     file = open('store.txt', 'r')
     tmpfile = open('tmp_store.txt', 'w')
     flag = True
     for l in file:
          field = l.split('\t')
          if field[0] == name :
               flag = False
               name = input('Enter New Name for '+field[0]+' : ')
               price = input('Enter the new Price for '+name+' : ')
               l = name +'\t'+ price +'\n'
          tmpfile.write(l)
     file.close()
     tmpfile.close()
     os.remove('store.txt')
     os.rename('tmp_store.txt', 'store.txt')
     if flag : print('This Product Not in the store\n-------------------------------')
     else: print('Product edit Successfully\n-------------------------------')



def get_price(name):
     with open('store.txt', 'r') as file:
          for l in file:
               field = l.split('\t')
               if field[0] == name : return int(field[1]) 

def add_to_reset():
    with open('reset.txt', 'a') as file:
          c = 'y'
          while c == 'y':
               name = input('Enter product name : ')
               if exist(name):
                    if exist_in_reset(name): add_exist_product(name)
                    else :
                         quantity = int(input('The quantity you want to buy : '))
                         price = quantity*get_price(name)
                         file.write(name +'\t'+ str(quantity) +'\t'+ str(price) +'\n')
                         print("Product purchased successfully !\n-------------------------------")
               else : print('This product not in the store !')
               c = input('Do you want to buy again (y / n) : ')
               print('-------------------------------')


def show_reset():
     with open('reset.txt', 'r') as file:
          print('Name\tQ\tprice\n-------------------------------')
          for l in file: print(l, end = '')
     print('-------------------------------')

def search_in_reset():
     name = input("Enter Product Name to search : ")
     with open('reset.txt', 'r') as file:
          flag = True
          for l in file:
               field = l.split('\t')
               if field[0] == name :
                    print('Found\nName\tQ\tPrice\n-------------------------------')
                    print(l, end = '')
                    flag = False    
          if flag : print('This Product Not purchased\n-------------------------------')

def remove_from_reset():
     name = input("Enter Product Name to return : ")
     file = open('reset.txt', 'r')
     tmpfile = open('tmp_reset.txt', 'w')
     flag = True
     for l in file:
          field = l.split('\t')
          if field[0] == name :
               flag = False
          else :
               tmpfile.write(l)
     file.close()
     tmpfile.close()
     os.remove('reset.txt')
     os.rename('tmp_reset.txt', 'reset.txt')
     if flag : print('This Product Not purchased\n-------------------------------')
     else: print('Product returned successfully\n-------------------------------')


print('\n\nWelcome to my amazing store ! Asem Gadu\n-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-')
print('1.You are saler/stuff\n2.You are customer')
x = input('Your option num : ')
print('-------------------------------')
if x == '1' :
     ch = 'y'
     while ch == 'y':
          print('1.To add product to store')
          print('2.To show all product in store')
          print('3.To search product in store')
          print('4.To remove product from store')
          print('5.To edit product in store')
          n = input('Your option num : ')
          print('-------------------------------')
          if n == '1' : add_product()
          elif n == '2' : show_store()
          elif n == '3' : search_product()
          elif n == '4' : remove_product()
          elif n == '5' : update_product()
          else: print('invaled num !\n-------------------------------')
          ch = input('Do you want to do another process ? (y / n) : ')
          print('-------------------------------')
elif x == '2':
     print('Welecom in my store dear customer !\nThis are all our products that is available today\n')
     show_store()
     ch = 'y'
     while ch == 'y':
          print('1.To buy a product')
          print('2.To view all your purchases')
          print('3.To show all product in store')
          print('4.To search for product in store')
          print('5.To search for product in reset')
          print('6.To return product')
          n = input('your option num : ')
          print('-------------------------------')
          if n == '1' : add_to_reset()
          elif n == '2' : show_reset()
          elif n == '3' : show_store()
          elif n == '4' : search_product()
          elif n == '5' : search_in_reset()
          elif n == '6' : remove_from_reset()
          else: print('invaled num !\n-------------------------------')
          ch = input('Do you want to do another process ? (y / n) : ')
          print('-------------------------------')
else : print('invaled num !')
print('-------------------------------')
print('good bye !')