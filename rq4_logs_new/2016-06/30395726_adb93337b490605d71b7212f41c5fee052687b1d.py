string = raw_input()
param = raw_input().split(' ')  
print string[:int(param[0])] + param[1] + string[int(param[0]) + 1:]