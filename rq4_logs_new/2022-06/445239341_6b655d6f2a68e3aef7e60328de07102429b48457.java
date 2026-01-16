package password.vault.api;

public class ServerTextCommandsFactory {
    public static String registerCommand(String username, String email, String password, String repeatedPassword,
                                   String masterPassword,
                                   String repeatedMasterPassword) {
        return String.format("%s %s %s %s %s %s %s",
                             ServerCommand.REGISTER.getCommandText(), username, email, password, repeatedPassword,
                             masterPassword, repeatedMasterPassword);
    }

    public static String loginCommand(String username, String password) {
        return String.format("%s %s %s", ServerCommand.LOGIN.getCommandText(), username, password);
    }

    public static String logoutCommand() {
        return ServerCommand.LOGOUT.getCommandText();
    }

    public static String disconnectCommand() {
        return ServerCommand.DISCONNECT.getCommandText();
    }

    public static String helpCommand() {
        return ServerCommand.HELP.getCommandText();
    }

    public static String addPasswordWithCheck(String website, String usernameForSite, String password,
                                                String masterPassword) {
        return String.format("%s %s %s %s %s",
                             ServerCommand.ADD_PASSWORD_WITH_CHECK.getCommandText(), website, usernameForSite, password,
                             masterPassword);
    }

    public static  String removePassword(String website, String usernameForSite, String masterPassword) {
        return String.format("%s %s %s %s",
                             ServerCommand.REMOVE_PASSWORD.getCommandText(), website, usernameForSite, masterPassword);
    }

    public static String retrieveCredentials(String website, String usernameForSite, String masterPassword) {
        return String.format("%s %s %s %s",
                             ServerCommand.RETRIEVE_CREDENTIAL.getCommandText(), website, usernameForSite,
                             masterPassword);
    }

    public static String generatePassword(String website, String usernameForSite, int safePasswordLength,
                                    String masterPassword) {
        return String.format("%s %s %s %s %s",
                             ServerCommand.GENERATE_PASSWORD.getCommandText(), website,
                             usernameForSite, safePasswordLength, masterPassword);
    }
}
