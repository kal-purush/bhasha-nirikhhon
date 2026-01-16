M=[[0,1,2,0,0,0],[1,0,0,2,0,3],[2,0,0,0,0,3],[0,2,0,0,2,0],[0,0,0,2,0,3],[0,3,3,0,3,0]]
#M is connection matrix with entries= distances b/w adjacent vertices
INF=9999999999
UNDEF=7
V=[['white',INF,UNDEF],['white',INF,UNDEF],['white',INF,UNDEF],['white',INF,UNDEF],['white',INF,UNDEF],['white',INF,UNDEF]]
#V is list of vertices. white= unvisited, red= visited. 2nd arg is current known shortest dist from start. initializes to infinity

def dist(m,n): return M[m][n]
#dist b/w adj vertices

def mpl(s):
    #min path length from start to end vertex
    V[s]=['red',0,s]
    while not(V[0][0]==V[1][0]==V[2][0]==V[3][0]==V[4][0]==V[5][0]=='red'):
        whtvert=[]#white vertices
        for i in range(6):
            if V[i][0]=='white':
                whtvert.append(i)
        print whtvert
        mindist=INF # dist of closest white vert from s
        u=s
        p=u
        for i in range(len(whtvert)):
            if V[whtvert[i]][1]<mindist:
                mindist=V[whtvert[i]][1]
                u=whtvert[i]
                p=i# index to be popped from whtvert
        print 'mindist', mindist
        print 'u',u
        V[u][0]='red'
        whtvert.pop(p)
        nbrs=[] #nbrs of u
        for j in range(0,6):
            print "j", j
            print "M[u][j]", M[u][j]
            if M[u][j]!=0 :
                nbrs.append(j)
            print "nbrs" ,nbrs              
        for v in nbrs:
            print "v" ,v
            if V[v][0]=='white':
                alt= V[u][1] + dist(u,v)
                if alt< V[v][1]:
                    V[v][1] = alt
                    V[v][2] = u

                
    