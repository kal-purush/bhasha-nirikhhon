

# def find_prime(num):
#     result =[]
#     for number in range(2,num+1):
#         for factor in range(2, int(number**0.5)+1):
#             if number%factor == 0:
#                 break   
#         else: 
#             result.append(number)
        
#     return result

def find_prime_factor(num):
    result = []
    factor = 2

    while (factor <= num):
        if num % factor == 0:
            num = num/factor
            result.append(factor)
        else: 
            factor +=1
    
    return result
