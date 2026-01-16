package global;

import model.DataStore;
import model.User;

public class Auth {

	private static User currentUser;
	
	public static User getCurrentUser() {
		return currentUser;
	}
	
	public static boolean login(String userName, String password) {
		User existingUser = DataStore.checkPasswordAndGetUser(userName, password);
		
		if(existingUser == null) {
			return false;
		}
		
		Auth.currentUser = existingUser;
		
		
		return true;
	}
	
	public static void logout() {
		currentUser = null;
	}
	
}