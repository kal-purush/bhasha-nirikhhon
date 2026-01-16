def find_path(u, v, prod, visited, G):

    if u==v:
        return prod
    visited.add(u)
    res = -1 

    for x in G[u]:
        w = x[0]
        val = x[1]
        if w not in visited:
            res = find_path(w, v, prod*val, visited, G)
            if res!=-1:
                break 

    visited.remove(u)
    return res 
    
def calc_eqn(eqns, vals, qs):

    G = {}
    n = len(eqns)

    for i in range(n):
        e = eqns[i]
        v = vals[i]
        G[e[0]] = G.get(e[0], []) + [(e[1],v)]
        G[e[1]] = G.get(e[1], []) + [(e[0],1/v)]
    
    for q in qs: 

        if q[0]==q[1] and q[0] in G.keys():
            print("1") 
        elif q[0] not in G.keys() or q[1] not in G.keys():
            print("-1")
        else:
            print(find_path(q[0], q[1], 1, set(), G))
        
calc_eqn([["a","b"],["b","c"]], [2.0,3.0], [["a","c"],["b","a"],["a","e"],["a","a"],["x","x"]])