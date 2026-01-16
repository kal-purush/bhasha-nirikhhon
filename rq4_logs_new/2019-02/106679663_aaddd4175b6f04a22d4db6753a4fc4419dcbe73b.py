import sys

def camelcase(s):
    # Complete this function
    count = 0
    if len(s)>0:
        count = 1
    for i in range(len(s)):
        if s[i]>='A' and s[i]<='Z':
            count += 1
    return count

if __name__ == "__main__":
    s = raw_input().strip()
    result = camelcase(s)
    print result