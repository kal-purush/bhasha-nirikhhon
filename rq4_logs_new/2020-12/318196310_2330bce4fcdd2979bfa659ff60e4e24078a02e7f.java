package be.upgrade.it.adventofcode2020.day6;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CustomDeclarationFormGroup {
    private final Set<Character> unique;

    public CustomDeclarationFormGroup(String input) {
        unique = new HashSet<>();
        for (char c : input.toCharArray()) {
            unique.add(c);
        }
    }

    public final int getNumberOfYesQuestions(){
        return unique.size();
    }

    public static final int getTotalNumberOfYesQuestionsForAllGroups(String[] input){
        int sum = 0;
        StringBuffer buffer = new StringBuffer();
        for (String s : input) {
            if("".equals(s)){
                CustomDeclarationFormGroup customDeclarationFormGroup = new CustomDeclarationFormGroup(buffer.toString());
                sum += customDeclarationFormGroup.getNumberOfYesQuestions();
                buffer = new StringBuffer();
            } else {
                buffer.append(s);
            }
        }
        CustomDeclarationFormGroup customDeclarationFormGroup = new CustomDeclarationFormGroup(buffer.toString());
        sum += customDeclarationFormGroup.getNumberOfYesQuestions();

        return sum;
    }
}