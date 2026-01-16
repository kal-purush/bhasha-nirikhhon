package gti310.tp3.solver;

import gti310.tp3.parser.ConcreteParser;

import java.util.List;

import data.Aretes;
import data.AretesIn;

public class ConcreteSolverNic {
	
	private ConcreteParser parser;
	private List<Aretes> listeAretes;
	private int nbSommets;
	private int sommetDepart; 
	private final int width = 2;
	private AretesIn areteIn;
	
	private int[][] graphe;

	public Object solve(Object input) {
		// TODO Auto-generated method stub
		parser = (ConcreteParser) input;
		areteIn = parser.getAretesIn();
		listeAretes = parser.getAretesIn().AretesIn;
		nbSommets = areteIn.nbSommets;
		sommetDepart = areteIn.sommetDepart;
		InitialiserSource();
		System.out.println(nbSommets);
		for(int i = 1; i <= nbSommets; i++){
			//distance
			System.out.println(i);
			//System.out.println(listeAretes.size());
			for(int j = 0; j < listeAretes.size(); j++){
				
				//System.out.println(j);
				
				//System.out.println(listeAretes.size());
				if(listeAretes.get(j).getDestination()== i){
					//System.out.println("destination liste arrete: "+ listeAretes.get(j).getParent()+ " graphe parent: " + i);
					relaxer(i, listeAretes.get(j), j);
				}
			}
		}
		/*for(int k = 0; k < listeAretes.size(); k++){
			if(graphe[destination][0]){
				relaxer();
			}
		}*/
		return null;
	}
	
	private void InitialiserSource( ){
		graphe = new int[nbSommets][width];
		for(int i = 0; i < nbSommets; i++){
			//distance
			if(i == sommetDepart){
				graphe[i][0] = 0;
			}
			else{
				graphe[i][0] = 999999;
			}
		}
	}
	private void relaxer(int parent, Aretes destination, int indexDestination){
		
		System.out.println("graphe poids parent: " + destination.getDestination() + " " + graphe[parent-1][0]);
		System.out.println(" destination poids: " +  " " + destination.getPoids());
		System.out.println("poids destiantion " + (parent-1) +  " " + graphe[destination.getParent()][0] );
		if((graphe[parent-1][0] +  destination.getPoids()) < graphe[destination.getParent()][0]){
			graphe[destination.getParent()][0] = graphe[parent-1][0] +  destination.getPoids();
			
			
			System.out.println("destination liste arrete: "+ destination.getDestination()+ " graphe parent: " + parent);
			System.out.println(graphe[destination.getParent()][0]);
		}
		
	}
}