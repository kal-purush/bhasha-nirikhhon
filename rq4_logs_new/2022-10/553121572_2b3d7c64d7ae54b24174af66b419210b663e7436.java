package Exception;
import java.util.Objects;

public class Data {

    public static boolean validateCredentials(String login,
                                              String password,
                                              String confirmPassword) {
        boolean loginValid = isLengthInRange(login,1,20) && isSymbolsValid(login);
        if (!loginValid) {
        throw new WrongLoginException("Неправильный логин");
        }

        boolean passwordValid = isLengthInRange(password, 1, 20)
                && isSymbolsValid(password)
                && Objects.equals(password, confirmPassword);
        if (!passwordValid) {
            throw new WrongPasswordException("Неправильный пароль");
        }
        return true;
    }

    private static boolean isLengthInRange(String value, int min, int max){
        if(value == null){
            return false;
        }
       int length = value.length();
       return length >= min && length <= max;
    }

    private static boolean isSymbolsValid(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (char c : value.toCharArray()) {
          boolean symbolMatches = (c >= 'a' && c <= 'z')
                  || (c >= 'A' && c <= 'Z')
                  || (Character.isDigit(c))
                  || (c == '_');
            if (!symbolMatches) {
                return false;
            }
          }
        return true;
        }
    }
