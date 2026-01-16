package be.upgrade.it.adventofcode2020.day2;

import be.upgrade.it.adventofcode2020.Utils;

import java.util.Arrays;

public class PasswordValidator {

    public long getNumberOfValidPasswords(String path){
        String[] input = Utils.resourceToStringList(path);
        return Arrays.stream(input).filter(PasswordValidator::isValidPassword).count();
    }

    public long getNumberOfValidPasswordsNewPolicy(String path){
        String[] input = Utils.resourceToStringList(path);
        return Arrays.stream(input).filter(PasswordValidator::isValidNewPolicyPassword).count();
    }

    public static boolean isValidPassword(String entry){
        int firstDivider = entry.indexOf("-");
        int secondDivider = entry.indexOf(" ");
        int thirdDivider = entry.indexOf(":");

        int min = Integer.parseInt(entry.substring(0, firstDivider).trim());
        int max = Integer.parseInt(entry.substring(firstDivider + 1, secondDivider).trim());
        char character= entry.charAt(secondDivider+1);
        String password = entry.substring(thirdDivider + 1).trim();

        int count = 0;
        for (char c : password.toCharArray()) {
            if(c == character){
                count++;
            }
        }

        return min <= count && count <= max;
    }

    public static boolean isValidNewPolicyPassword(String entry){
        int firstDivider = entry.indexOf("-");
        int secondDivider = entry.indexOf(" ");
        int thirdDivider = entry.indexOf(":");

        int position1 = Integer.parseInt(entry.substring(0, firstDivider).trim());
        int position2 = Integer.parseInt(entry.substring(firstDivider + 1, secondDivider).trim());
        char character= entry.charAt(secondDivider+1);
        String password = entry.substring(thirdDivider + 1).trim();

        char one = password.charAt(position1-1);
        char two = password.charAt(position2-1);

        return (one == character && two != character) || (one != character && two == character);
    }
}