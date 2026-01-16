package examples.cardGame;

public class Card {
	private String type;
	private int number;
	
	public Card(String type, int number) {
		this.type = type;
		this.number = number;
	}
	
	public int getNumber() {
		return this.number;
	}
	public String getType() {
		return this.type;
	}

	@Override
	public String toString() {
		
		String strNumber = number + "";
		if(number == 1) {
			strNumber = "A";
		} else if(number == 11) {
			strNumber = "J";
		} else if(number == 12) {
			strNumber = "Q";
		} else if(number == 13) {
			strNumber ="K";
		}
		
		return type + " [" + strNumber + "]";
	}
	
	
	

}