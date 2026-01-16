package mastermind;

public enum Colour {
	RED("r"), BLUE("b"), YELLOW("y"), GREEN("g"), ORANGE("o"), PINK("p");

	private String colour;

	Colour(String colour) {
		this.colour = colour;
	}

	public static boolean isValid(String colour) {
		for (Colour c : Colour.values()) {
			if (c.toString().equals(colour)) {
				return true;
			}
		}
		return false;
	}

	public static String validColours() {
		String rs = "";
		for (Colour c : Colour.values()) {
			rs += c.toString();
		}
		return rs;
	}

	public String toString() {
		return this.colour;
	}
}