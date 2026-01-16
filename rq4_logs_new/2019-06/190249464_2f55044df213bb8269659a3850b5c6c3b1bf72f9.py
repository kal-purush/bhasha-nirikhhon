N=int(input())
string=input()
vowels=["a","e","i","o","u"]
for i in string:
    if(i in vowels):
       string=string.replace(i,"")
print(string[::-1])