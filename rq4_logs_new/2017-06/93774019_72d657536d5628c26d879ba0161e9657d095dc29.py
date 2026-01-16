# Shape files have street data, name type , shape, one way or two way, 
# streets are linestring type. 

# to visualize the maps: QGIS

import pandas
import networkx
import numpy
import pyomo
import pyomo.opt
import pyomo.environ as pe 
import matplotlib.pyplot as plt
%matplotlib inline
import geoplotter
import cplex

class generateNetwork:
    def __init__(self):
        self.austin = pandas.read_csv("/Users/mridulamaddukuri/Dropbox/python/Computational Optimization/HW05/hw05_files/austin.csv")
        self.austin['node_start'] = self.austin.kmlgeometry.str.extract('LINESTRING \(([0-9-.]* [0-9-.]*),')
        self.austin['node_end'] = self.austin.kmlgeometry.str.extract(',([0-9-.]* [0-9-.]*)\)')
        self.austin['streets'] = zip(self.austin.node_start,self.austin.node_end,self.austin.MILES,self.austin.SECONDS)
        self.addresses = pandas.read_csv("/Users/mridulamaddukuri/Dropbox/python/Computational Optimization/HW05/hw05_files/addresses.csv")
        
    def network(self):
        austin_network = networkx.DiGraph()
        for start,end, dist,time in self.austin.streets[self.austin.ONE_WAY == 'FT']:
            austin_network.add_edge(start, end, length=dist, time = time)
        for start,end, dist,time in self.austin.streets[self.austin.ONE_WAY == 'TF']:
            austin_network.add_edge(end, start, length=dist,time = time)
        for start,end, dist,time in self.austin.streets[self.austin.ONE_WAY == 'B']:
            austin_network.add_edge(start, end, length=dist,time = time)
            austin_network.add_edge(end, start, length=dist,time = time)
        # nodes
        nodes = set(self.austin.node_end) | set(self.austin.node_start)
        
        for node in nodes:
            start = node.find('\(') +1
            end = node.find(' ',start)
            end2 = node.find('\)',end)
            austin_network.add_node(node,lat =float(node[end+1:end2]) , lon =float(node[start:end]))
        #austin_network.add_nodes_from(nodes)
        austin_network.edge_styles = {'default':{'color':'blue','linewidth':0.075}}
        austin_network.node_styles = {'default':{'marker':'None', 'linewidth':0.0002}}
        return austin_network
    
    def plotAustin(self):
        # function to plot the map using geoplotter
        network = self.network()
        self.plorr = geoplotter.GeoPlotter()
        self.plorr.drawNetwork(network)
        # etc
        self.plorr.drawPoints(-97.734530370784057,30.290428827385249,color = 'green',marker = '.',s = 0.2)
        # marking the points in addresses. Yes, there's a better way to do it
        self.plorr.drawPoints(-97.369789772206047,30.419373131365926,color = 'red',marker = '.',s = 0.2) #14
        self.plorr.drawPoints(-97.783804338218317,30.296127054801854,color = 'red',marker = '.',s = 0.2) #13
        self.plorr.drawPoints(-97.646889690523551,30.386262025789605,color = 'red',marker = '.',s = 0.2) #12
        self.plorr.drawPoints(-97.742602371076003,30.278862032926224,color = 'red',marker = '.',s = 0.2) #11
        self.plorr.drawPoints(-97.750585801124672,30.270296360399477,color = 'red',marker = '.',s = 0.2) #10
        self.plorr.drawPoints(-97.751630177018242,30.244246986457146,color = 'red',marker = '.',s = 0.2) #09
        self.plorr.drawPoints(-97.681867099369626,30.36861967459647,color = 'red',marker = '.',s = 0.2) #8
        self.plorr.drawPoints(-97.881170922999829,30.245461027594001,color = 'red',marker = '.',s = 0.2) #7
        self.plorr.drawPoints(-97.760150783664713,30.257584246730939,color = 'red',marker = '.',s = 0.2) #6
        self.plorr.drawPoints(-97.750270462861252,30.247946277901271,color = 'red',marker = '.',s = 0.2) #5
        self.plorr.drawPoints(-97.749592241597199,30.249822080046911,color = 'red',marker = '.',s = 0.2) #4
        self.plorr.drawPoints(-97.746609472963897,30.41539337333516,color = 'red',marker = '.',s = 0.2) #3
        self.plorr.drawPoints(-97.738199685461055,30.263668053767404,color = 'red',marker = '.',s = 0.2) #2
        self.plorr.drawPoints(-97.750624593036889,30.247023805390022,color = 'red',marker = '.',s = 0.2) #1
        self.plorr.drawPoints(-97.749937017287508,30.24892417332487,color = 'red',marker = '.',s = 0.2) #0
       
    def chooseClosest(self,lat,lon):
        austinNetwork = self.network()
        nodes = pandas.Series(austinNetwork.nodes())
        lon_all = (nodes.str.extract('([0-9-.]* )')).astype('float')
        lat_all = (nodes.str.extract('( [0-9-.]*)')).astype('float')
        lon_all = numpy.array(lon_all)
        lat_all = numpy.array(lat_all)
        dist = numpy.sqrt((lon_all - lon)**2 + (lat_all - lat)**2 )
        idx = numpy.argmin(dist)
        return nodes[idx]
    
    def getSPNetworkx(self,startnode,destnode):
        startnode = self.chooseClosest(self.addresses.Lat[startnode],self.addresses.Lon[startnode])
        destnode = self.chooseClosest(self.addresses.Lat[destnode],self.addresses.Lon[destnode])
        #networkx.shortest_path(network111,startnode,destnode ,weight='time')
        nodes_path = pandas.DataFrame(networkx.shortest_path(network111,startnode,destnode ,weight='time'))
        nodes_path.columns = ["node"]
        nodes_path['lon_all'] = (nodes_path.node.str.extract('([0-9-.]* )')).astype('float')
        nodes_path['lat_all'] = (nodes_path.node.str.extract('( [0-9-.]*)')).astype('float')
        nodes_path['final'] = zip(nodes_path.lon_all,nodes_path.lat_all)
        path = list(nodes_path.final)
        self.plorr.drawLines([path], linewidth = 0.1,color = "orange")
        
    def getSPCplex(self,startnode,destnode):
        startnode = self.chooseClosest(self.addresses.Lat[startnode],self.addresses.Lon[startnode])
        destnode = self.chooseClosest(self.addresses.Lat[destnode],self.addresses.Lon[destnode])
        self.m = pe.ConcreteModel()
        net = self.network()
        self.m.node_set = pe.Set(initialize = sorted(net.nodes()))
        self.m.arcs_set = pe.Set(initialize = sorted(net.edges()),dimen = 2)
        self.m.Y = pe.Var(self.m.arcs_set, domain=pe.NonNegativeReals)
        
        def obj_rule(m):
            return sum(m.Y[e] * net.edge[e[0]][e[1]]['time'] for e in self.m.arcs_set)
        
        self.m.OBJ = pe.Objective(rule=obj_rule, sense=pe.minimize)
        
        # Flow Ballance rule
        def flow_bal_rule(m, n):
            preds = net.predecessors(n)
            succs = net.successors(n)
            return (sum(m.Y[(p,n)] for p in preds) - sum(m.Y[(n,s)] for s in succs) == 0-1*int(n==startnode) +1*int(n==destnode))
        self.m.FlowBal = pe.Constraint(self.m.node_set, rule=flow_bal_rule)
    
        solver = pyomo.opt.SolverFactory("cplex")
        results = solver.solve(self.m, tee=True, keepfiles=False, options_string="mip_tolerances_integrality=1e-9 mip_tolerances_mipgap=0")

        if (results.solver.status != pyomo.opt.SolverStatus.ok):
            logging.warning('Check solver not ok?')
        if (results.solver.termination_condition != pyomo.opt.TerminationCondition.optimal):  
            logging.warning('Check solver optimality?') 
        
        current_node = startnode
        path = []
        path.append(current_node)
        while current_node != destnode:
            for n in net.successors[current_node]:
                if int[self.m.Y[current_node,n].value ==1]:
                    current_node = n
                    path.append(current_node)
                    break
        self.plorr.drawLines([path], linewidth = 0.1,color = "orange")
        return path


if __name__ == '__main__':
    new = generateNetwork()
    new.plotAustin()
    new.getSPNetworkx(15,13)
    plt.savefig("HulaHut.pdf")
    plt.clf()
    new.getSPNetworkx(15,3)
    plt.savefig("Rudys.pdf")
    plt.clf()
    new.getSPCplex(15,13)