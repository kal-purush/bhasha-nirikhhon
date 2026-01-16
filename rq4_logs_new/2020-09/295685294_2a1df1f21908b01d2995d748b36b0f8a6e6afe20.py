import math

def line():
  print('===================')

def sum_of_elements(arr, n): 
    sumfirst = 0; 
    sumsecond = 0; 
  
    for i in range(n): 
        # add elements in first half sum 
        if (i < n // 2): 
            sumfirst += arr[i]; 
        # add elements in the second half sum 
        else: 
            sumsecond += arr[i]; 

    line()          
    if sumfirst == sumsecond:
      print("Lucky ticket")
    else:
      print("Not lucky. Try enter again")

    # print("Sum of first half elements is",  
    #                 sumfirst, end = "\n"); 
    # print("Sum of second half elements is",  
    #                 sumsecond, end = "\n"); 
    # print("Sum =", sumfirst + sumsecond)  
    
num = int(input('Enter pair amount of digits: '))

# printing number  
arr = [int(x) for x in str(num)] 
  
n = len(arr); 

sum_of_elements(arr, n); 











# if (int(math.log10(n))+1)%2 == 0:
#   print(int(math.log10(n))+1)
# suma = 0
# # print(n // 10, '= n // 10')
# # print(n % 10, '= n % 10')
# while n > 0:
#     digit = n % 10
#     suma = suma + digit
#     n = n // 10
    

# print("Сумма:", suma)