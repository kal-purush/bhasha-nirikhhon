package Model.StaticGraph;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;

public class GetKeyPlayers {
    public void getKP(Graph theGraph, int keyPlayerNum, String attribute){

        for (Node node : theGraph.getEachNode()){
            if (node.hasAttribute(attribute+"KeyPlayer")){
                node.removeAttribute(attribute+"KeyPlayer");
            }
        }

        CalDegree calDegree;
        CalCloseness calCloseness;
        CalBetweenness calBetweenness;

        int graphSize = theGraph.getNodeCount();

        if (attribute.equals("Degree")){
            calDegree = new CalDegree();
            calDegree.CalDegree(theGraph);

        }else if (attribute.equals("Closeness")){
            calCloseness = new CalCloseness();
            calCloseness.CalCloseness(theGraph);

        }else if (attribute.equals("Betweenness")){
            calBetweenness = new CalBetweenness();
            calBetweenness.CalBetweenness(theGraph);
        }




        Node[] keyPlayers = new Node[keyPlayerNum];
        Node[] allPlayers = new Node[graphSize+1];
        allPlayers[graphSize] = theGraph.getNode(graphSize-1);

        for (int num=0; num<graphSize; num++){
            allPlayers[num] = theGraph.getNode(num);
//            System.out.println(node.getId()+" has degree: "+node.getAttribute("Degree"));
        }


        for (int k=0; k<keyPlayerNum+1; k++){
            for (int i=0; i<k+1; i++){ // find kth biggest degree node
                int maxIndex = i;
                int maxValue = allPlayers[i].getAttribute(attribute);
                for (int j=i+1; j<graphSize; j++){
                    if ((Integer)allPlayers[j].getAttribute(attribute) > maxValue){
                        maxIndex = j;
                        maxValue = allPlayers[j].getAttribute(attribute);
                        swap(allPlayers,i,maxIndex);
                    }
                }
            }
            if (k<keyPlayerNum){
                keyPlayers[k] = allPlayers[k];
                allPlayers[k].setAttribute(attribute+"KeyPlayer");
                System.out.println("KP: "+allPlayers[k]);
            }
        }

    }

    private void swap (Node[] nodes, int i, int j) {
        Node temp = nodes[i];
        nodes[i] = nodes[j];
        nodes[j] = temp;
    }
}