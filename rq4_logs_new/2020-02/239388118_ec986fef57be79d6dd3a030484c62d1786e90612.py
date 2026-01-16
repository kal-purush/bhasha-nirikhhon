import pycuber as pc
import json
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle


faces_fig = plt.figure(figsize=[6,6])
face_size=0.05

cubo = pc.Cube()

fsd = lambda c: c.perform_algo("F R U R' U' F'")
fsi = lambda c: c.perform_algo("F' L' U' L U F")
asd = lambda c: c.perform_algo("F U R U' R' F'")
asi = lambda c: c.perform_algo("F' U' L' U L F")

colors = {'o' : 'orange',
          'b' : 'aqua',
          'w' : 'white',
          'r' : 'deeppink',
          'y' : 'yellow',
          'g' : 'chartreuse'} # dict colors lindos



#def draw_face(f):
#    for row in range(3):
#            for col in range(3):
#                print(uface[row][col],end='')
#            print()

def draw_face(f, xy=[0,0]):
    
    line_size = 1
    cubie_size = 9
    cube_size = 3 * cubie_size + 4 * line_size

    box = [xy[0], xy[1], face_size, face_size]  #left, bottom, width, height   
    ax = faces_fig.add_axes(box,xlim=(0,cube_size),ylim=(0,cube_size),label=f)
    base = Rectangle((0,0),cube_size,cube_size,linewidth=0,facecolor='black')
    ax.add_patch(base)
    ax.axis('off')
    ax.set_zorder(1)

    
    for i in range(3):

        b = line_size + (cubie_size + line_size) * (2-i) # bottom
        l = line_size   # left
        
        for j in range(3):
            print("Placing cubie {} in ({},{})".format(f[i],l,b))
        
            r = Rectangle((l,b), cubie_size, cubie_size,
                          linewidth=0,edgecolor='black',facecolor=colors[f[i*3+j]])
            ax.add_patch(r)

            l += cubie_size + line_size

    return ax

def hash_face(f):
    cs = ''
    for r in f:
        for s in r:
            cs += s.colour[0]
    return cs

def hash_upface(c):
    uf = c.get_face('U')
    return hash_face(uf)



def loop_cube(c, visited):

    st = hash_upface(c)

    if st in visited:
        print('!'+st)
        return {}

    print('+'+st)
    
    fsd(c)
    to_der = hash_upface(c)
    asd(c)

    fsi(c)
    to_izq = hash_upface(c)
    asi(c)

    asd(c)
    fr_der = hash_upface(c)
    fsd(c)

    asi(c)
    fr_izq = hash_upface(c)
    fsi(c)

    visited[st] = {'to_der' : to_der,
                   'from_der' : to_izq,
                   'to_izq' : fr_der,
                   'from_izq' : fr_izq}

    fsd(c)
    visited.update(loop_cube(c, visited))
    asd(c)

    fsi(c)
    visited.update(loop_cube(c, visited))
    asi(c)

    return visited


def map_as_dict(g):

    nodes = [{'id' : k, 'label' : k, 'img' : draw_face(k)} for k in g]

    edgs = []
    for k in g:
        edgs.append( (k,g[k]['to_der']) )
        edgs.append( (k,g[k]['to_izq']) )
        edgs.append( (k,g[k]['from_der']) )
        edgs.append( (k,g[k]['from_izq']) )

    edges = [{'id' : s+'-'+t, 'source' : s, 'target' : t} for (s,t) in edgs]

    return {'nodes' : nodes,'edges' : edges}

def graphiphy(g):

    G = nx.Graph()
    for n in g['nodes']:
        G.add_node(n['id'], img=n['img'])

    for e in g['edges']:
        G.add_edge(e['source'], e['target'])
    
    return G

#def draw(g):
#    plt.subplot(111)
#    nx.draw(g)


def draw_graph(g):
    
    ax=plt.axes()
    faces_fig.add_axes(ax)
    ax.axis('off')
    ax.set_aspect('equal')
    #ax.set_zorder(0.01)
    ax.zorder = 0.01

    # Layout
    pos=nx.spring_layout(g)     #, pos = {'yyyyyyyyy' : (0,0)}) #, fixed=['yyyyyyyyy'])
    #{k: array([x., y.]), k:...}
    
    
    
    nx.draw_networkx_edges(g,pos,ax=ax)
    
    plt.xlim(-1,1)  # data coords
    plt.ylim(-1,1)  # data coords
    

    trans=ax.transData.transform
    trans2=faces_fig.transFigure.inverted().transform
    dlt=0.05/2
    
    for n in g:
        
        xx,yy=trans(pos[n])     # figure coordinates
        xa,ya=trans2((xx,yy))   # axes coordinates

        faces_fig.add_axes(g.node[n]['img'])
        g.node[n]['img'].set_position([xa-dlt,ya-dlt, 0.05, 0.05])
        
        
        #a = plt.axes([xa-p2,ya-p2, piesize, piesize])
        #a.set_aspect('equal')
        #a.imshow(G.node[n]['image'])
        #a.axis('off')



if __name__ == '__main__':
    mapa = loop_cube(cubo, {})
    dm = map_as_dict(mapa)
    nxg = graphiphy(dm)
    draw_graph(nxg)
    plt.show()
    #input()

    
    #cb = draw_face('yyybrgrgb')
    
    #plt.show()
    
        

