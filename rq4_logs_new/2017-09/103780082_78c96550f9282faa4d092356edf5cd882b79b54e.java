
public enum BackWood {
	
	INDIAN_ROSEWOOD("IndianRoseWood"),
	BRAZILLIAN_ROSEWOOD("BrazillianRoseWood"),
	MAHOGANY("Mahogany"),
	MAPLE("Maple"),
	COCOBOLO("Cocobolo"),
	CEDAR("Cedar"),
	ADIRONDACK("Adirondack"),
	ALDER("Alder"),
	SITKA("Stika");
	
	public final String eBackWood;
	
	BackWood(String backWood){
		
		eBackWood = backWood;
	}
	
	public String geteBackWood() {
		
		return eBackWood;
		
	}

}