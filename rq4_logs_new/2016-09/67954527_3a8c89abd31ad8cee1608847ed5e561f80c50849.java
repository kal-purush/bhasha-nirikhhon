/**
 * 
 */
package ro.jademy;

import java.util.ArrayList;

/**
 * @author Andrei
 * 
 */
public class Agentie {

	public String numeAgentie;
	private ArrayList<PachetSejur> oferta = new ArrayList<PachetSejur>();

	public Agentie() {

	}

	public Agentie(String nume) {
		this.numeAgentie = nume;

	}

	public void addSejur(int index, Sejur sejur) {
		
		if (index>=0){
			boolean FOUND = false ;
			for (PachetSejur ps : oferta) {
				if (ps.getIndex()==index){
					FOUND = true ;
					ps.addSejur(sejur);
					break;
				}
					
			}
			
			if (!FOUND){
				PachetSejur ps = new PachetSejur();
				ps.setIndex(index);
				ps.addSejur(sejur);
				oferta.add(ps);
			}
			
		}
		
		
	/*
	 	if (index >= 0 && index < oferta.size()) {
			oferta.get(index).addSejur(sejur);
			return;
		}
		if (index >= 0 && index >= oferta.size()) {
			int cateLipsesc = index - oferta.size();

			for (int i = 0; i <= cateLipsesc; i++) {
				PachetSejur ps = new PachetSejur();
				oferta.add(ps);
			}
			oferta.get(index).addSejur(sejur);
		}
	*/

	}

	public String pretSejur() {
		String informatiiSejur ="   ";
		for (PachetSejur ps  : oferta) {
			informatiiSejur +=
					"Sejurul numarul " + ps.getIndex() + " are o valoare de " + 
					ps.pretPachet() + " euro.\n   ";
		
		}

		return informatiiSejur;
	}

}