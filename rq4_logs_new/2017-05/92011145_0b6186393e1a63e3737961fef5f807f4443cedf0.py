#!/usr/bin/env python
# -*- coding:utf-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

def mail(user):
    flag = True
    try:
        sender = '786896996@qq.com'
        msg = MIMEText('邮件内容...', 'plain', 'utf-8')
        msg['From'] = formataddr(['肉球', '786896996@qq.com'])
        msg['To'] = formataddr(['肉球', '2816849947@qq.com'])
        msg['Subject'] = '主题是什么'

        server = smtplib.SMTP('smtp.qq.com', 25)
        server.login(sender,'809011lcp')
        server.sendmail(sender,[user,],msg.as_string())
        server.quit()
    except Exception:
        flag = False
    return flag

ret = mail('2816849947@qq.com')
if ret:
    print('成功')
else:
    print('失败')


#http://www.cnblogs.com/xcblogs-python/p/5727238.html

def say(): #定义函数
    print('hello, python')
    return 123  #函数返回值
f = say  #把函数名赋给另一个变量，f指向这个函数
f()      #执行函数
say()    #执行函数
value = f()   #拿到返回值
print(value)


#默认参数必须放在指定参数后面
def show(a1, a2=1, a3=3):
    print('ok')


#指定参数
def show2(a1, a2):
    print(a1, a2)
show2(a2=1,a1=3)


#动态参数
def show(*arg):  #一个*把传入的参数当成元祖来处理
    print(arg,type(arg))
show(1,2,3)

def show(**arg):  #两个*把传入的参数当成字典来处理
    print(arg,type(arg))
show(n1=99,bb='bb')

def show(*args, **kwargs):  #两个*必须放在一个*之后
    print(args,type(args))
    print(kwargs,type(kwargs))
show(11, 22, 33, n1=22, ll='ll')  #实参也要一一对应
l = [11, 22, 33, ]
d = {'n1': 22, 'll': 'll'}
show(*l, **d)



#动态参数实现格式化
s1 = '{0} is {1}'
s2 = '{name} is {actor}'
l = ['lcp', 'ok']
d = {'name': 'lcp', 'actor': 'ok'}
result = s1.format(*l)
result2 = s2.format(**d)
print(result)
print(result2)



#lambda表达式
#创建形式参数a
#函数内容a+1,并把结果return
fun = lambda a: a+1   #:前面的为形式参数，后面为函数体
ret = fun(99)
print(ret)