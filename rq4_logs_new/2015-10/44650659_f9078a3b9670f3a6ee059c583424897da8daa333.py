#coding=utf-8
#!/usr/bin/python

from ctypes import cdll

hello_module=cdll.LoadLibrary("./hello.so")
#print(dir(cdll))
hello_module.hello()
print(hello_module.sum(1,2))