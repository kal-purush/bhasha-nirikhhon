package ga.model;

import java.util.ArrayList;

/**
 * 
 * @author rubens
 * Os genes do cromossomos serão correspondentes aos scripts que serão utilizados no portfólio.
 * Os genes serão representados como números inteiros iniciando em 0. 
 * Cada gene corresponde à um script da classe BasicExpandedConfigurableScript
 */
public class Chromosome {
	private ArrayList<Integer> Genes;

	public Chromosome() {
		this.Genes = new ArrayList<>();
	}

	public ArrayList<Integer> getGenes() {
		return Genes;
	}

	public void setGenes(ArrayList<Integer> genes) {
		Genes = genes;
	}
	
	public void addGene(Integer gene){
		this.Genes.add(gene);
	}
	
	public void print(){
		System.out.print("Chromosome ");
		for (Integer gene : Genes) {
			System.out.print(gene+" ");
		}
		System.out.println("");
	}
	
}