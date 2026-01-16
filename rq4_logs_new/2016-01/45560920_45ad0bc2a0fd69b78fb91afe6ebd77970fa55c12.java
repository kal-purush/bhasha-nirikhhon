package diffusion;

import core.components.Edge;
import edu.uci.ics.jung.graph.Graph;

import java.util.ArrayList;
import java.util.Collection;

/**
 * class that holds diffusion mechanisms for graphs to undergo information dissemination
 * Mechanisms include:
 *
 *                              Initial Cascade
 *
 * the graph has an infection probability P, each infected node has probability P to
 * infect its neighbours. Once it tries to infect a neighbour once it never tries to
 * infect it again.
 *
 * Initially no nodes are infected but a seed is chosen to infect at time t=0 who will
 * begin the infection. After this there are subsequent infections at each step a node
 * that was infected in the previous rounds tries to infect an un-infected neighbours
 * this process continues until no new nodes are infected.
 *
 *
 *                              Linear Threshold
 *
 * This model represents infection as having a large enough number of neighbours being infected
 * so then a node would succumb to the infection.
 *
 * The graph/nodes are defined with a threshold T where if enough of a nodes neighbours
 * are infected such that T < (# of infected neighbours/total # of neighbours).
 *
 *
 *                                  Notes
 *
 * All mechanisms must be passed graphs with the corresponding vertex types for their diffusion.
 * Random graph generation is not supported as the generators only create the generic vertex type
 */
public class DiffusionMechanism {

    public DiffusionMechanism(){}

    /**
     * Initial cascade diffusion mechanism which models diffusion as
     * an agent based phenomenon between vertices
     * @param G Graph made of ICVertices
     * @param seed the initial set of vertices to set as infection
     * @return a string of results, data includes; timestep, Total Infected, Newly Infected
     * @throws IllegalArgumentException
     */
    public String InitialCondition(Graph<ICVertex,Edge> G, Collection<ICVertex> seed) throws IllegalArgumentException{

        int time = 0; //the infection timestep
        int size = 0; //total size of the infection

        String stats = "timestep,Total Infected,Newly Infected \n"; //string to hold basic stats of the diffusion
        stats += time +","+ size +","+ seed.size() +"\n";

        //check to see proper seed graph combination was passed
        seed.stream()
                .forEach(e -> {
                    if(!G.containsVertex(e)){
                        throw new IllegalArgumentException("a vertex among your seed does not exist in the graph");
                    }
                });

        //infect all seed vertices
        seed.stream().forEach(e -> e.setStatus(true));

        ArrayList<ICVertex> infected = new ArrayList<>(seed); //handle for all vertices that will infect this timestep
        ArrayList<ICVertex> newlyInfected = new ArrayList<>(); //vertices that will be infected this timestep

        //infection loop
        while (!infected.isEmpty()){

            for (ICVertex v: infected){
                newlyInfected.addAll(v.infect(G)); //for each infected node attempt to infect neighbours
            }

            size += infected.size();
            infected.clear();
            infected.addAll(newlyInfected);

            stats += ++time +","+ size +","+ infected.size() +"\n";

            newlyInfected.clear();

        }

        return stats;

    }

    /**
     * Linear Threshold model, which defines an infection event as having a
     * favourable ratio of infected neighbours. with respect to some threshold
     * value
     *
     * @param G Graph made of LTVertices
     * @param seed the initial set of vertices to set as infection
     * @return a string of results, data includes; timestep, Total Infected, Newly Infected
     */
    public String LinearCascade(Graph<LTVertex,Edge> G, Collection<LTVertex> seed){
       
        ArrayList<LTVertex> susceptible = new ArrayList<>();
        int time = 0; //the infection timestep
        int size = seed.size(); //total size of the infection

        String stats = "timestep,Total Infected,Newly Infected \n"; //string to hold basic stats of the diffusion
        stats += time +","+ 0 +","+ seed.size() +"\n";

        //infect all seed vertices
        seed.stream().forEach(e -> e.setStatus(true));

        //population of all nodes susceptible of infection
        for (LTVertex v:G.getVertices()){
            if(!seed.contains(v)){
                susceptible.add(v);
            }
        }

        //check to see proper seed graph combination was passed
        seed.stream()
                .forEach(e -> {
                    if(!G.containsVertex(e)){
                        throw new IllegalArgumentException("a vertex among your seed does not exist in the graph");
                    }
                });

        
        do{
            time++;
            ArrayList<LTVertex> toInfect = new ArrayList<>();

            //for each node that can be infected determine if it will be
            for(LTVertex v1 : susceptible){
                double influence = 0.0;

                //count all infected neighbours
                for (LTVertex v2:G.getNeighbors(v1)){
                    if(v2.getStatus()){
                        influence++;
                    }
                }

                //calculate influence on a node
                influence = influence/(double)G.degree(v1);

                //determine if it gets infected
                if(v1.getThreshhold() < influence){
                    toInfect.add(v1);
                }
            }

            stats += time +","+ size +","+ toInfect.size() +"\n";

            //if no new infections at one step no new infections will occur, so break
            if(toInfect.size() == 0){
                break;
            }

            //set infection status nodes
            for (LTVertex v3: toInfect){
                v3.setStatus(true);
            }

            size += toInfect.size();

            //adjust lists
            susceptible.removeAll(toInfect);
            toInfect.clear();

            //adds stats for break case which wouldnt be covered otherwise
            if(susceptible.isEmpty()){
                time++;
                stats += time +","+ size +","+ toInfect.size() +"\n";
            }

        }while(!susceptible.isEmpty()); //when all nodes infected break

        return stats;

    }

}