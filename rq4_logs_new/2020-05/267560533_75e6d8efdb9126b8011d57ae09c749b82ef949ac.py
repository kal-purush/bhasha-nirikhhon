import os
import sys
import threading 
#sys.setrecursionlimit(2**20)



def DFS(G,i):
    global t ; global is_explored;global leader ;global f 
    is_explored[i] = True
    leader[i]=s
    try:

        for j in G[i]:
            if is_explored[j]==False:
          
                DFS(G,j)
        t =t +1
        f[i]=t
    except:
        t= t  + 1
        f[i]=t
    #except BaseException  as e :
    #    print(e)
        

def DFS_loop( G):
    
    #n = sorted( G.keys(), reverse = True)
    n = len(G) 
    global s 
    leader={}
    for i in range(n-1,-1,-1):
        if is_explored[i] == False:
            s = i 
            DFS(G,i)  


if __name__ =="__main__":
    threading.stack_size(67108864)        
    sys.setrecursionlimit(2 ** 20)        
    thread = threading.Thread(target=main)       
    thread.start()
    t = 0  ; f =dict() ; s = None ; leader = dict(); 
    '''    G = {};G_new = {} 
    with open('text.txt') as ff:
        for row in ff:
            ver1,ver2 = row.strip().split(' ',1)
            ver1,ver2 = int(ver1),int(ver2)
            #if ver2 ==283: print("found")
            if ver2 not in G_new:
                G_new[ver2] = [];G_new[ver2].append(ver1)
            else:
                G_new[ver2].append(ver1)


    '''

    num_nodes=875714
    G = [[] for row in range(num_nodes)]
    G_new = [[] for row in range(num_nodes)]
    is_explored = [False] * num_nodes
    with open('SCC.txt') as ff:
        for row in ff:
            ver1,ver2 = row.strip().split(' ' ,1)
            ver1,ver2 = int(ver1)-1,int(ver2)-1
            G_new[ver2]+=[ver1]


   # kosaraju's two pass algorithm 
    #print(G_new[283])
    DFS_loop(G_new)
    print("finish reverse: and magical ordering length:      "+ str(len(f)))  
    t= 0 ; s=None; is_explored = [False]*num_nodes; leader = dict()
    # get magical ordering f
    #print(f[253])
    '''    with open('text.txt') as ff :
        for row in ff:
            ver1,ver2 = row.strip().split(' ')
            ver1,ver2 = int(ver1),int(ver2)
            key1 = f[ver1]; key2 = f[ver2]
            if key1  not in G:
                G[key1] = [];G[key1].append(key2)
            else:
                G[key1].append(key2)
    '''
    G_new =[]
    #print(f)
    with open('SCC.txt') as ff:
        for row in ff:
            
            ver1,ver2 = row.strip().split(' ' ,1)
            ver1= int(ver1)-1;ver2=int(ver2)-1
            #print(ver1,ver2)
            ver1,ver2 = f[ver1],f[ver2]
            G[ver1-1]+= [ver2-1]

    
    DFS_loop(G) 
    rst ={};rst2=[]
    for key,value in leader.items():
        if value not in rst:
            rst[value]=[];rst[value].append(key) 
        else:
            rst[value].append(key)
    
    for key in rst.keys():
        rst2.append(len(rst[key]))
    #print(rst2)
    #print(f)
    #print(rst)
    rst2= sorted(rst2,reverse=True)
    print(rst2[:5])
        
   