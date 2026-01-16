#10 : Programs on Closure, Callback and Decorators

# 1.Write a simple decorator which adds dollar sign before number to extend the functionality of day_earnings. 
# def day_earnings(number):
#     print(number)

def adder(fn):
    symbol = '$'
    def state(n):
        print("My today earnings is " + symbol,end='')
        fn(n)
    return state

@adder
def day_earnings(number):
    print(number)

day_earnings(300)
day_earnings(11000)








