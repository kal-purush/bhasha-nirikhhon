#Hash Tables: Ransom Note
#https://www.hackerrank.com/challenges/ctci-ransom-note/problem?h_l=interview&playlist_slugs%5B%5D=interview-preparation-kit&playlist_slugs%5B%5D=dictionaries-hashmaps


def checkMagazine(magazine, note):
    len_m = len(magazine)
    len_n = len(note)
    if len_n > len_m:
        print('No')
        return False
    else:
        dic_m = {}
        for w in magazine:
            if dic_m.get(w, 0):
                dic_m[w] += 1
            else:
                dic_m[w] = 1
        
        for w in note:
            #print(w, w in dic_m)
            if dic_m.get(w,0) > 0:
                dic_m[w] -= 1    
            else:
                print('No')
                return False
                
        print('Yes')
        return True

#Evaluate:
#Runtime = O(n+m)
#Space = O(n)