import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

style.use('fivethirtyeight')

G = nx.DiGraph()
fig = plt.figure()
ax1 = fig.add_subplot(1,1,1)

relationshiplistcopy = ""


def animate(i):

    relationshiplist = open('data.txt','r').read()
    relationshiplist = eval(relationshiplist)

    if i != 0:
        if relationshiplistcopy == relationshiplist:
            return
        pass

    


    G.clear()
    G.add_edges_from(relationshiplist)

    
    pos = nx.spring_layout(G)
    nx.draw_networkx_nodes(G, pos, cmap=plt.get_cmap('jet'), node_size = 500)
    nx.draw_networkx_labels(G, pos)
    nx.draw_networkx_edges(G, pos, edge_color='r', arrows=True)
    nx.draw_networkx_edges(G, pos, arrows=False)

    relationshiplistcopy = relationshiplist
    

ani = animation.FuncAnimation(fig, animate, interval=9000)
plt.show()