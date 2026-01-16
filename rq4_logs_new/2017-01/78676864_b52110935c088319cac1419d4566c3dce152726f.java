
public class Dobbelstenen {
	private int aantalDobbelstenen = 5;
	
	private int [] waarden = new int [aantalDobbelstenen];
	private String [][] positie = {{"a","x"},{"b","x"},{"c","x"},{"d","x"},{"e","x"}};
	private boolean vast[] = new boolean[aantalDobbelstenen];
	private int totaalWaarde;
	private int aantalWaarden = 6;

	//testmethodes
	public void printVast(){
		for (int i = 0; i < aantalDobbelstenen; i++) {
			System.out.println(vast[i]);
		}
	}


	public void worp(){
		
		for (int i = 0; i < aantalDobbelstenen; i++) {
			//als de dobbelsteen niet is vastgelegd nogmaals gooien
			if (vast[i]==false){
				waarden[i] = (int)(Math.random()*aantalWaarden)+1;
				
			}
		}
	}
	
	public int getTotaalWaarde(){
		totaalWaarde = 0;
		for (int i = 0; i < aantalDobbelstenen; i++) {
			totaalWaarde += waarden[i];
		}
		return totaalWaarde;
		
	}

	public void printWaarden(){
		for (int i = 0; i < waarden.length; i++) {
			if (vast[i] == true){
				System.out.print("  " + positie[i][1] + "  ");
			} else {
				System.out.print("  " + positie[i][0] + "  ");
			}
		}
		System.out.println();
		for (int i = 0; i < waarden.length; i++) {
			System.out.print(" [" + waarden[i] + "] ");
		}
		System.out.println("\nTotaal: " + getTotaalWaarde());
		System.out.println();

	} 

	public int getAantalDobbelstenen(){
		return aantalDobbelstenen;
	}

	public String[][] getPositie(){
		return positie;
	}

	public void setVast(int index){
		vast[index] =! vast[index];
	}

	public boolean[] getVast(){
		return vast;
	}
	
	public int getAantalWaarden(){
		return aantalWaarden;
	}
	
	public int[] getWaarden(){
		return waarden;
	}

}