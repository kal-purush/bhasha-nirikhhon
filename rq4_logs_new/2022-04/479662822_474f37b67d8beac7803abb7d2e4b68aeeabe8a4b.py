import os
import random
import cv2
import networkx
from itertools import combinations
import networkx as nx
import matplotlib.pyplot as plt
import pandas
import pandas as pd
import openpyxl
from openpyxl import Workbook
import numpy as np
##########################  Values    ######################
HC_nx = 0
p = 0.5
q = 0.5
#theta
cost = 0.005
##########################  Values    ######################
#Return the density of a graph.
def density(G):


    #The density for undirected graphs is

    n = number_of_nodes(G)
    m = number_of_edges(G)
    if m == 0 or n <= 1:
        den = 0.0
    else:
        den = m*2.0/float(n*(n-1))
    return den

def number_of_nodes(G):
    #"""Return the number of nodes in the graph."""
    return G.number_of_nodes()

def reachable_nodes(G, start):
    seen = set()
    stack = [start]
    while stack:
        node = stack.pop()
        if node not in seen:
            seen.add(node)
            stack.extend(G.neighbors(node))
    return seen

def is_connected(G):
    start = next(iter(G))
    reachable = reachable_nodes(G, start)
    return len(reachable) == len(G)

def number_of_edges(G):
    #"""Return the number of edges in the graph. """
    return G.number_of_edges()

def ProbabilityOfSharingResources(nx, G, sourceNode, destinationNode):
    HC_nx = nx.harmonic_centrality(G)
    try:
        shortestPath = nx.shortest_path(G, source=sourceNode, target=destinationNode, method='dijkstra')
        shortesPathInt = len(shortestPath) - 1
    except:
        shortesPathInt = 0
        ##print("error")
    # Constant variables
    pIntoOneQ = p * (1 - q)
    if shortesPathInt != 0:
        temp = pIntoOneQ * ((1 / shortesPathInt) / HC_nx.get(destinationNode))
        return temp
    else:
        return 0

def FinalValue(nodeList, val, G, nx):
    multiPlication = 1
    for x in nodeList:
        if x != val:
            probabilityOfResourceSharingNodes = ProbabilityOfSharingResources(nx, G, val, x)
            nodeWiseProbability = 1 - probabilityOfResourceSharingNodes
            multiPlication = multiPlication * nodeWiseProbability

    return (1 - multiPlication)

def main():
# To create an empty undirected graph
    font1 = {'family': 'serif', 'color': 'blue', 'size': 20}
    font2 = {'family': 'serif', 'color': 'darkred', 'size': 15}
    theta = 0.1

    #######################     Final Data For Different Paprameters Value
    finalListMinRA = {}     #MinResourceAvaialability
    finalListMaxRA = {}     #MaxResourceAvaialability
    finalListMaxCent = {}   #MaxCentrality
    finalListMinCent = {}   #MinCentrality
    finalListMaxDens = {}  # Density
    finalListMinDens = {}  # Density
    finalListAPL = {} # Average Path Length
    finalListTransit = {} # Transitivity
    finalConnectedness= {}
    #######################     Final Data For Different Paprameters Value


    while theta <= 0.6:
        print(theta)
        iterations = 0
        totalConnect = 0
        totalNonConnect = 0
        totalPercentConnect = 0
        dataListMinResourceAvaialability = []
        dataListMaxResourceAvaialability = []
        dataListMinCentrality = []
        dataListMaxCentrality = []
        dataDensityList = []
        dataAvaragePathList = []
        dataTransitivitylist = []
        notconnect = 0
        connect = 0
        percentConnect = 0
        ##########################End Data Analysis
        while iterations < 2:


            G = networkx.Graph()
            #nodeList = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35' , '36', '37', '38', '39', '40']
            nodeList = ['A', 'B', 'C', 'D', 'E', 'F' , 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O']#, 'P', 'Q', 'R', 'S', 'T']
            for x in nodeList:
                G.add_node(x)

            pIntoOneMinusq = q * (1 - p)

            CostByTheta = cost / theta
            """
            print("\n")
            print("q(1-p)", pIntoOneMinusq)
            print("c/theta: ", CostByTheta)
            """
            nodeKeyValuePair = {}
            random.shuffle(nodeList)
            comb = combinations(nodeList, 2)
            comb = list(comb)
            random.shuffle(comb)
            #print(comb)
            for i in list(comb):
                val = i[0]

                firstNodeInitialValue = FinalValue(nodeList, val, G, nx)
                d = {val: firstNodeInitialValue}
                nodeKeyValuePair.update(d)
                #print("\n")
                #print("Taking ", val, " as main node")
                #print(nodeKeyValuePair)

                x = i[1]
                if x != val:
                    #print(val, " vs ", x)
                    secondNodeInitialResourceAvailability = FinalValue(nodeList, x, G, nx)
                    G.add_edge(val, x)
                    firstNodeAfterAddingEdgeResourceValue = FinalValue(nodeList, val, G, nx)
                    secondNodeAfterAddingEdgeResourceAvailability = FinalValue(nodeList, x, G, nx)
                    if (((pIntoOneMinusq * (firstNodeAfterAddingEdgeResourceValue - firstNodeInitialValue)) > CostByTheta) and
                        (pIntoOneMinusq * (secondNodeAfterAddingEdgeResourceAvailability - secondNodeInitialResourceAvailability)) > CostByTheta):
                        firstNodeInitialValue = firstNodeAfterAddingEdgeResourceValue
                        additionKvp = {val: firstNodeInitialValue}
                        add2Kvp = {x: secondNodeAfterAddingEdgeResourceAvailability}
                        nodeKeyValuePair.update(additionKvp)
                        nodeKeyValuePair.update(add2Kvp)
                        edgeAdded = True
                        #print("Added edge between", val, " and ", x)
                        #print(nodeKeyValuePair)
                    else:
                        G.remove_edge(val, x)
                        #print('Link addition not possible between', val, 'and', x)
            #print(nodeKeyValuePair)

            ######################   Analysis Properties
            # Convert dictionary nodeKeyValuePair to finalResAvailList
            finalResAvailabilityList = list(nodeKeyValuePair.values())
            ##print(finalResAvailabilityList)
            finalResAvailabilityList.sort()
            #print("Minimum Resource Availability", finalResAvailabilityList[0])
            minResourceAvaialability =  finalResAvailabilityList[0]
            #print("Maximum Resource Availability", finalResAvailabilityList[-1])
            maxResourceAvaialability = finalResAvailabilityList[-1]
            #End-Segment conversion
            # Print Harmonic Centrality of each node
            HC_nx = nx.harmonic_centrality(G)
            centralityList = []
            for node in nodeList:
                # print('Node', node, 'Closeness ', HC_nx.get(node))
                centralityList.append(HC_nx.get(node))
            centralityList.sort()
            #print("Minimum Centrality", centralityList[0])
            #print("Maximum Centrality", centralityList[-1])
            mincentrality = centralityList[0]
            maxcentrality = centralityList[-1]

            os.chdir('D:/PycharmProjects/StaticNetFormation/images')

            nx.draw(G, with_labels=True)
            #plt.savefig(str(p) + '.png')
            plt.savefig(str(theta)+"%d.png"%(iterations))
            plt.clf()
            """
            m = number_of_edges(G)
            print("\n")
            print('Number of Edges', m)
            print("\n")
            """
            den = density(G)
            #print('Density', den)
            connectFlag = is_connected(G)
            if(connectFlag == True):
                #print('Average Path', nx.average_shortest_path_length(G))
                #print('Transitivity', nx.transitivity(G))
                avgPath = nx.average_shortest_path_length(G)
                transtiv = nx.transitivity(G)
                connect = connect+1
            else:
                avgPath = 0
                transtiv = 0
                #print("Graph is Not Connected")
                notconnect= notconnect +1
            ######################   Analysis Properties
            #list.insert(iterations, elem)
            dataListMaxResourceAvaialability.insert(iterations, maxResourceAvaialability)
            dataListMinResourceAvaialability.insert(iterations, minResourceAvaialability)
            dataListMaxCentrality.insert(iterations, maxcentrality)
            dataListMinCentrality.insert(iterations, mincentrality)
            dataDensityList.insert(iterations, den)
            dataAvaragePathList.insert(iterations, avgPath)
            dataTransitivitylist.insert(iterations, transtiv)

            G.clear()
            iterations = iterations+1
            percentConnect = percentConnect +1

        #print("Min Resource Availability Index:", dataListMinResourceAvaialability)
        #print("Max Resource Availability Index:", dataListMaxResourceAvaialability)
        #print("Max Centrality Index:", dataListMaxCentrality)
        #print("Min Centrality Index:", dataListMinCentrality)
        print("Data Desnsity Index:", dataDensityList)
        #print("Average Path Index:", dataAvaragePathList)
        #print("Transitivity Index:", dataTransitivitylist)

        finalListMaxDens[theta] = max(dataDensityList)
        finalListMinDens[theta] = min(dataDensityList)


        finalConnectedness
        if(connect ==0):
            print("Percentage of Connectedness:", 0)
            print("Number of times Connected:", connect)
        else:
            print("Percentage of Connectedness:", ((connect * 100) / percentConnect))
            print("Number of times Connected:", connect)
        ##############################################
        finalConnectedness[theta] = (connect * 100)/ percentConnect

        theta = theta + 0.1


    print(finalListMaxDens)
    print(finalListMinDens)
    print(finalConnectedness)

    col1 = "Theta"
    col2 = "Max_Density"
    col3 = "Min_Density"
    col4 = "Connect_Percent"
    dataPanda = pd.DataFrame({col1: finalListMaxDens.keys(), col2: finalListMaxDens.values(), col3: finalListMinDens.values()})
    dataPanda1 = pd.DataFrame({col1: finalConnectedness.keys(), col4: finalConnectedness.values()})

    dataPanda.to_excel('D:/PycharmProjects/StaticNetFormation/sample_data.xlsx', sheet_name='sheet1', index=False)
    dataPanda1.to_csv('D:/PycharmProjects/StaticNetFormation/connect_data.csv', index=False)
    #print(dataPanda)
    var = pd.read_excel("D:/PycharmProjects/StaticNetFormation/sample_data.xlsx")
    var1 = pd.read_csv("D:/PycharmProjects/StaticNetFormation/connect_data.csv")
    #plt.plot(var['Theta', 'Max Density'])
    #var.plot()
    #plt.show()
    ############################# Density Figure Properties
    x = var.Theta
    y1 = var.Max_Density
    y2 = var.Min_Density
    # Plot a horizontal line
    #plt.title("Benefit Vs Density", fontdict = font1)
    plt.rcParams['font.size'] = '16'
    #fig = plt.figure(figsize=(19.20,10.80))
    plt.xlabel("Benefit("r"$\beta$)", fontdict = font2)
    plt.ylabel("Density", fontdict = font2)
    plt.plot(x, y1, '-.r', linewidth=2, label='Max Density', marker='o')
    plt.plot(x, y2, '--b', linewidth=2, label='Min Density', marker='o')
    plt.legend()
    plt.savefig('P='+str(p)+'Q='+str(q)+'C='+str(cost)+'Density.png')
    plt.savefig('P='+str(p)+'Q='+str(q)+'C='+str(cost)+'Density.eps', dpi=600)
    plt.show()
    ############################# Coneectedness Figure Properties
    x1 = var1.Theta
    y3 = var1.Connect_Percent
    # Plot a horizontal line
    #plt.title("Benefit Vs Density", fontdict = font1)
    plt.rcParams['font.size'] = '16'
    #fig = plt.figure(figsize=(19.20,10.80))
    plt.xlabel("Benefit("r"$\beta$)", fontdict = font2)
    plt.ylabel("Connectedness(%)", fontdict = font2)
    plt.plot(x1, y3, '-.r', linewidth=2, label='Connectedness', marker='o')
    #plt.plot(x, y2, '--b', linewidth=2, label='Min Density')
    plt.legend()
    plt.savefig('P='+str(p)+'Q='+str(q)+'C='+str(cost)+'Connectedness.png')
    plt.savefig('P='+str(p)+'Q='+str(q)+'C='+str(cost)+'Connectedness.eps')
    plt.show()

    ############################  End Figure Properties
# Using the special variable
# __name__
if __name__ == "__main__":
    main()