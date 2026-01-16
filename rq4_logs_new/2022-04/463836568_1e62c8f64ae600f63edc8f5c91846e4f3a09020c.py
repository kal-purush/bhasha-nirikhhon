#Closure: クロージャ

#関数(function)もオブジェクト
def compute_square(num):
    return num * num


f = compute_square
print(type(f))
print(f(10))

function_list = ["1", 1, True, f]
print(function_list[-1](10))

def execute_func(func, param):
    return func(param)


print(execute_func(f, 10))

#関数をreturnする
def return_func():

    def inner_func():
        print("This is an inner function")
    return inner_func()

f = return_func()
print(f)


#Closure: 状態をキープした関数を作ることができる
#状態が静的
def power(exponent):
    def inner_power(base):
        return base ** exponent
    return inner_power


power_two = power(2)
print(power_two(3))

#状態が動的
def average():
    nums = []
    def inner_average(num):
        nums.append(num)
        print(len(nums))
        return sum(nums) / len(nums)
    return inner_average

averag_nums = average()
print(averag_nums(5))
print(averag_nums(2))