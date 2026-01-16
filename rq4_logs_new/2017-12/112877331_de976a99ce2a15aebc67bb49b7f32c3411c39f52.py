def listgame( A):
    n = len(A)
    scores = [ [ 0 for x in range(n)] for y in range(n)]
    h = 0
    for gap in range(n):
        i = 0
        j = gap
        for j in range(j,n):
            x = scores[i + 2][j] if ( (i + 2) <= j) else 0
            y = scores[i + 1][j - 1] if ((i +1) <= (j-1)) else 0
            z = scores[i][j - 2] if (i <= (j - 2)) else 0
            print("x:",x,"y:",y,"z:",z)
            scores[i][j] = max( A[i] + min(x,y), A[j] + min(y,z))
            i += 1
    return scores
 
def listgameb( A):
    n = len(A)
    scores = [ [ 0 for x in range(n)] for y in range(2)]
    h = 0
    for gap in range(n):
        i = 0
        j = gap
        for j in range(j,n):
            if(i%2 == 0):
                x = scores[0][j] if ( (i + 2) <= j) else 0
                y = scores[0][j-1] if ((i +1) <= (j-1)) else 0
                z = scores[0][j-2] if (i <= (j - 2)) else 0
                print("x:",x,"y:",y,"z:",z)
                scores[1][j] = max( A[i] + min(x,y), A[j] + min(y,z))
            else:
                x = scores[1][j] if ( (i + 2) <= j) else 0
                y = scores[1][j-1] if ((i +1) <= (j-1)) else 0
                z = scores[1][j-2] if (i <= (j - 2)) else 0
                print("x:",x,"y:",y,"z:",z)
                scores[0][j] = max( A[i] + min(x,y), A[j] + min(y,z))                
            i += 1
    return scores

def main():
    arr = [8,15,3,7]
    result = listgameb(arr)
    result1 = listgame(arr)
    
    print(result)
    
    n = len(result)
    for i in range(n):
        for j in range(n):
            print( result[i][j], " ", end="")
        print("")
    print("##########")
    for i in range(n):
        for j in range(n):
            print( result1[i][j], " ", end="")
        print("")
main()