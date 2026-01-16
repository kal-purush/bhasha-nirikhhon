import java.security.SecureRandom;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PasswordHelper {
    protected static char[] createUniquePassword() {
        String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lower = "abcdefghijklmnopqrstuvwxyz";
        String special = "!@#$%&";
        String numbers = "0123456789";
        String all = upper + lower + special + numbers;
        Random random = new SecureRandom();
        int length = 8;
        char[] password = new char[length];

        password[0] = lower.charAt(random.nextInt(lower.length()));
        password[1] = upper.charAt(random.nextInt(upper.length()));
        password[2] = special.charAt(random.nextInt(special.length()));
        password[3] = numbers.charAt(random.nextInt(numbers.length()));

        for (int i = 0; i < length; i++) {
            password[i] = all.charAt(random.nextInt(all.length()));
        }

        for (int i = 0; i < password.length; i++) {
            int randomPosition = random.nextInt(password.length);
            char temp = password[i];
            password[i] = password[randomPosition];
            password[randomPosition] = temp;
        }

        System.out.println(password);
        return password;
    }


    public static String generateRandomPassword() {

        int length = 8;
        final String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%&";

        Random random = new SecureRandom();

        return IntStream.range(0, length)
                .map(i -> random.nextInt(chars.length()))
                .mapToObj(randomIndex -> String.valueOf(chars.charAt(randomIndex)))
                .collect(Collectors.joining());
    }


}