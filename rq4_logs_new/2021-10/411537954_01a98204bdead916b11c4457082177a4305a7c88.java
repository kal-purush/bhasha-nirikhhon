package cooper.verification;

import cooper.ui.Ui;
import cooper.verification.roles.UserRole;

import java.util.HashMap;

public class Login extends AccessMethod {

    public Login(UserDetails userDetails) {
        super(userDetails);
    }

    @Override
    public boolean isRegisteredUser(HashMap<String, UserRole> registeredUsers) {
        return registeredUsers.containsKey(userDetails.getUsername());
    }

    public boolean hasCorrectRole(HashMap<String, UserRole> registeredUsers) {
        // compares user role which is already in hashmap tp user role of current am object
        UserRole userRoleInHashMap = registeredUsers.get(userDetails.getUsername());
        return userRoleInHashMap.equals(userDetails.getUserRole());
    }

    public void askUserToRegister() {
        Ui.showPleaseRegisterMessage();
    }
}