#Count Triplets
#https://www.hackerrank.com/challenges/count-triplets-1/problem?h_l=interview&playlist_slugs%5B%5D=interview-preparation-kit&playlist_slugs%5B%5D=dictionaries-hashmaps

def countTriplets(arr, r):
    count = 0
    dic = {}
    dicPair = {}

    for i in reversed(arr):
        if i*r in dicPair:
            count += dicPair[i*r]
        if i*r in dic:
            dicPair[i] = dicPair.get(i, 0) + dic[i*r]
        
        dic[i] = dic.get(i, 0) + 1

    return count

#Evaluate
#runtime = O(n)
#space = O(n)