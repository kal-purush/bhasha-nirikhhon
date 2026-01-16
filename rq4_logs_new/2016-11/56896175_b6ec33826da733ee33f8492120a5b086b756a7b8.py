#Python中的除了map和reduce外，还有一些别的如filter, find, all, any的函数做辅助（其它函数式的语言也有），可以让你的代码更简洁，更易读

print '过程式编程：'
nums = [2, -5, 9, 7, -2, 5, 3, 1, 0, -3, 8]

positive_cnt = 0
positive_sum = 0;

for i in range(len(nums)):
    if nums[i] > 0:
        positive_cnt += 1
        positive_sum += nums[i]

if positive_cnt > 0:
    print positive_sum / positive_cnt
else:
    print"error"

print '函数式编程:'
positive_nums = filter(lambda x: x > 0, nums)
print reduce(lambda x,y:x+y, positive_nums) / len(positive_nums)


#过程式编程
print '过程式编程:'
from random import random

time = 5
car_pos=[1,1,1]

while time:
    time -= 1

    print 5 - time 

    for i in range(len(car_pos)):
        if random() > 0.3:
            car_pos[i] += 1;
        
        print '-'*car_pos[i]
             


#过程式编程封装:
#些函数间必需知道其它函数是怎么修改它们之间的共享变量的，所以，这些函数是有状态的。
print '过程式编程封装:'
def move_cars():
    for i, _ in enumerate(car_positions):
        if random() > 0.3:
            car_positions[i] += 1
 
def draw_car(car_position):
    print '-' * car_position
 
def run_step_of_race():
    global time
    time -= 1
    move_cars()
 
def draw():
    print 5 - time
    for car_position in car_positions:
        draw_car(car_position)
 
time = 5
car_positions = [1, 1, 1]
 
while time:
    run_step_of_race()
    draw()


#函数式编程：
print '函数式编程Functional Programming：'
def move_cars(car_positions):
    return map(lambda x: x + 1 if random() > 0.3 else x,
               car_positions)
 
def output_car(car_position):
    return '-' * car_position
 
def run_step_of_race(state):
    return {'time': state['time'] - 1,
            'car_positions': move_cars(state['car_positions'])}
 
def draw(state):
    print 5 - state['time']
    print '\n'.join(map(output_car, state['car_positions']))
 
def race(state):
    draw(state)
    if state['time']:
        race(run_step_of_race(state))

race({'time': 4,
      'car_positions': [1, 1, 1]})

#上面的代码依然把程序的逻辑分成了函数，不过这些函数都是functional的。因为它们有三个症状：
# 1）它们之间没有共享的变量。
# 2）函数间通过参数和返回值来传递数据。
# 3）在函数里没有临时变量。
 