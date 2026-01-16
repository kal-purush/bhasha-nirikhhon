package enumerations;

public enum BlackjackMove {
	HIT(1),
	STAND(2),
	DOUBLE(3),
	SPLIT(4),
	TAKE_INSURANCE(5),
	DONT_TAKE_INSURANCE(6),
	SURRENDER(7);
	
	private String hit = "H";
	private String stand = "S";
	private String doubleDown = "D";
	private String split = "P";
	private String takeInsurance = "I";
	private String dontTakeInsurance = "NI";
	private String surrender = "SU";
	
	int move;
	
	private BlackjackMove(int move) {
		this.move = move;
	}
	
	public String toString() {
		String string;
		
		switch (this) {
			case HIT:					string = hit;
										break;
			case STAND:					string = stand;
										break;
			case DOUBLE:				string = doubleDown;
										break;
			case SPLIT:					string = split;
										break;
			case TAKE_INSURANCE:		string = takeInsurance;
										break;
			case DONT_TAKE_INSURANCE:	string = dontTakeInsurance;
										break;
			case SURRENDER:				string = surrender;
										break;
			default:					string = "";
										break;
		}
		
		return string;
	}

}