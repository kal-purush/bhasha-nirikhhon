package it.polito.tdp.extflightdelays.model;

import java.util.Comparator;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

public class ComparaWeightedEdge extends DefaultWeightedEdge implements Comparator{
	SimpleWeightedGraph grafo;
	
	@Override
	public int compare(Object arg0, Object arg1) {
		DefaultWeightedEdge e1 = (DefaultWeightedEdge) arg0;
		DefaultWeightedEdge e2 = (DefaultWeightedEdge) arg1;
		
		// TODO Auto-generated method stub
		return 0;
	}
	
}