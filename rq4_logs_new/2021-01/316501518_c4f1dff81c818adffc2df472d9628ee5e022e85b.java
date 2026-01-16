package security;

import java.security.SecureRandom;

/**
 * Contains the methods meant to generate a random secure password.
 * 
 * @author Aitor Fidalgo
 */
public class RandomPasswordGenerator {
    
    /**
     * String containing all the possible characters contained in the random password.
     */
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                           + "abcdefghijklmnopqrstuvwxyz"
                                           + "0123456789*+-¿?(){}[]@#$&%";
    
    /**
     * Creates a random password of the specified lenght.
     * 
     * Appends random characters from {@link #CHARACTERS} to create a random String.
     * 
     * @param passwordLenght The specified lenght.
     * @return 
     */
    public static String getRandomPassword(int passwordLenght) {  
        SecureRandom random = new SecureRandom();
        StringBuilder stringBuilder = new StringBuilder();
        
        for(int i = 0; i < passwordLenght; i++) {
            int randomIndex = random.nextInt(CHARACTERS.length());
            stringBuilder.append(CHARACTERS.charAt(randomIndex));
        }
        
        return stringBuilder.toString();
    }
}