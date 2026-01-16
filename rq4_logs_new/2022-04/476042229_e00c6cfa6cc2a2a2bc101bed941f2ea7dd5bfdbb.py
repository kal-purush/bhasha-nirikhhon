def f(s):
    try:
        buff= s[0]
        count = 0
        bufflen=1
        stride =0
        while stride + bufflen <= len(s) :
            if buff == s[stride:stride+bufflen]:
                stride += bufflen
                count+=1
            else:
                count = 0
                buff = s[:bufflen+1]
                bufflen+=1
                stride =0
        return(buff+s[stride:stride+bufflen], count)
    except:
        print('tri again(:')


def fs(s):
    try:
        for i in range(1, len(s) + 1):
            t, k = s[:i], len(s) // i
            if t * k == s:
                return (t, k)
    except:
        print('tri again(:')


def is_palindrome(s):
    try:
        s=s.lower()
        txt=s
        txtr=txt[::-1]
        if txt == txtr:
            return  True
        else:
            return  False
    except:
        print('tri again(:')


def solve(a,b):
    try:
        x=''
        k=a.find('*')

        if a==b:
            return True
        elif '*' in a:
            if len(b)<len(a):
                g=a.replace('*','')
                if g==b:
                    return True
                else:
                    return False
            elif '*' in a:
                for i in a:
                    if i == '*':
                        for j in b[k:]:
                            x+=j
                            p=a.replace('*',x)
                            #print(p)
                            #print(x)
                            if p==b:
                                return True                            
                            else:
                                p=a
                if p!= b:
                    return False
                            
                                
            
        else:
            return False
    
    except:
        print('tri again(:')


def find_nth_occurrence(sub_string, string, occurrence=1):
    try:
        place = string.find(sub_string,0,len(string))
        if occurrence ==1:
            return place
        else:
            while occurrence >1:
                place = string.find(sub_string,place+1,len(string))
                occurrence-=1
                print(place)
        return place
    except:
        print('tri again(:')
