l1 = []
l2 = []
import random
def input_student():
    while True:
        n = input('请输入学生姓名: ')
        if not n:
            break 
        l1.append(n)
    return l1


def rand():
    if l1 == []:
        print('已经抽取完了')
    else:   
        x = random.choice(l1)
        l1.remove(x)
        l2.append(x)
    

def show():
    print('*******************')

    print('1.增加姓名          ')
    print('2.随机抽取          ')
    print('3.查看剩余名单       ')
    print('4.查看已抽取名单     ')
    print('5.退出              ')
    print('*******************')


def main():
    while True:
        show()
        try:
            m = int(input('请输入:'))
        except ValueError:
            print('输入错误')
        if m == 1:
            input_student()
        elif m == 2:
            rand()
        elif m == 3:
            print('剩余名单是:',l1)
        elif m == 4:
            print('已抽取名单是:',l2)
        elif m == 5:
            break

main()





# select country,name,gongji from sanguo where 
#     -> gongji in (
#     -> select max(gongji) from sanguo group by country)
#     -> group by country;
