package cooper.verification;

import java.util.HashMap;

public abstract class SignInProtocol {

    protected final SignInDetails signInDetails;

    public SignInProtocol(SignInDetails signInDetails) {
        this.signInDetails = signInDetails;
    }

    public abstract void executeSignIn(Verifier verifier, HashMap<String, UserRole> registeredUsers);

    public boolean isRegisteredUser(HashMap<String, UserRole> registeredUsers) {
        return registeredUsers.containsKey(signInDetails.getUsername());
    }
}