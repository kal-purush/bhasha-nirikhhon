package endpoints;

public interface SessionEndPoints {
    String signUp(final String emailAddress, final String password);
    String logIn(final String emailAddress, final String password);
    String logOut(final String emailAddress);
    String requestPasswordChange(final String emailAddress);
    String changePassword(final String token);
}