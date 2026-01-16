package cooper.verification;

import cooper.exceptions.InvalidArgumentException;
import cooper.exceptions.InvalidUserRoleException;
import cooper.exceptions.UnrecognisedCommandException;
import cooper.parser.CommandParser;
import cooper.ui.Ui;
import cooper.verification.roles.UserRole;

import java.util.HashMap;

public class Verifier {

    private final HashMap<String, UserRole> registeredUsers;
    private final CommandParser commandParser;

    public Verifier(HashMap<String, UserRole> registeredUsers, CommandParser commandParser) {
        this.registeredUsers = registeredUsers;
        this.commandParser = commandParser;
    }

    public void verify() {
        boolean canAccess = false;
        while (!canAccess) {
            String input = Ui.getInput();
            try {
                AccessMethod accessMethod = commandParser.parseLoginRegisterDetails(input);
                if (accessMethod.isRegisteredUser(registeredUsers)) {
                    if (accessMethod instanceof Login) {
                        canAccess = ((Login) accessMethod).hasCorrectRole(registeredUsers);
                    } else if (accessMethod instanceof Registration) {
                        ((Registration) accessMethod).askUserToLogin();
                    }
                } else {
                    if (accessMethod instanceof Login) {
                        ((Login) accessMethod).askUserToRegister();
                    } else if (accessMethod instanceof Registration) {
                        ((Registration) accessMethod).registerUser(registeredUsers);
                    }
                }
            } catch (UnrecognisedCommandException | InvalidArgumentException | InvalidUserRoleException e) {
                e.printStackTrace();
            }
        }
    }
}