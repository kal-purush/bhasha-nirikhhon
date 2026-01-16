package menu;

// displays a menu where user chooses via first letter of each item,
// with strings that have up to the same first 2 letters

public class LSLetterMenu extends LSMenu {
	
	public LSLetterMenu(String title) {
		super(title);
	}
	
	String displayMenuItem(int i) {
		String str = menu[i].getItemString();
		char id = menu[i].getItemID();
		
		// Basically checks to see where the ID character is and where it needs to put parentheses
		if (str.charAt(0) == id) {
			return "(" + id + ")" + str.substring(1) + " ";
			
		} else if (str.charAt(1) == id) {
			return str.substring(0,1) + "(" + id + ")" + str.substring(2) + " ";
			
		} else if (str.charAt(2) == id) {
			return str.substring(0,2) + "(" + id + ")" + str.substring(3) + " ";
			
		} else {
			return "error";
		}
	}
	
	// Lookup based on ID instead of based on ID character instead of number and string
	int lookUpMenuItem(String s) {
		char c = s.charAt(0);
		for (int i = 0; i < nItems; i++) {
			if (menu[i].getItemID() == c) {
				return i + 1;
			}
		}
		return MENU_ITEM_ERROR;
	}
}