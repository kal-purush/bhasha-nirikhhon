#信息加密
#给你个小写英文字符串a和一个非负数b(0<=b<26), 将a中的每个小写字符替换成字母表中比它大b的字母。
#这里将字母表的z和a相连，如果超过了z就回到了a。例如a="cagy",b=3, 则输出 fdjb
c = []
for i in a:
    
    
    i = ord(i) + b
    if i > ord('z'):
        i = i % ord('z') + 96
    c.append(i)
d = ''   
for i in c:
    d = d + chr(i)
    
print d