package be.upgrade.it.adventofcode2020.day4;

import java.util.*;
import java.util.regex.Pattern;

public class PassportValidator {
    private final Collection<String> batch;
    private final String[] mandatoryPassportKeys = {"byr", "iyr", "eyr", "hgt", "hcl", "ecl", "pid"};

    public PassportValidator(String[] lines) {
        batch = new ArrayList<>();

        StringBuffer buffer = new StringBuffer();

        for (String line : lines) {
            if("".equals(line)){
                String passport = buffer.toString().trim();
                batch.add(passport);
                buffer = new StringBuffer();

            } else {
                buffer.append(line).append(" ");
            }
        }

        if(buffer.length() != 0){
            batch.add(buffer.toString().trim());
        }
    }

    public long getNumberOfValidPassports(){
        return getNumberOfValidPassports(false);
    }

    public long getNumberOfValidPassports(boolean includeValueCheck){
        long count = 0L;
        for (String s : batch) {
            Map<String, String> passport = convertToMap(s);
            boolean check1 = containsAllFields(passport);
            boolean check2 = true;

            if(check1 && includeValueCheck){
                check2 = containsValidData(passport);
            }

            if(check1 && check2){
                count++;
            }
        }
        return count;
    }

    private Map<String, String> convertToMap(String s) {
        Map<String, String> map = new HashMap<>();
        String[] split = s.split(" ");
        for (String entry : split) {
            String[] keyValue = entry.split(":");
            map.put(keyValue[0], keyValue[1]);
        }
        return map;
    }

    private boolean containsAllFields(Map<String, String> passport){
        Set<String> keys = passport.keySet();
        return keys.containsAll(Arrays.asList(mandatoryPassportKeys));
    }

    private boolean containsValidData(Map<String, String> passport){
        for (Map.Entry<String, String> entry : passport.entrySet()) {
            switch (entry.getKey()){
                case "byr":
                    if(!matchesPattern(entry.getValue(), "19[2-9][0-9]|200[0-2]")){
                        return false;
                    }
                    break;

                case "iyr":
                    if(!matchesPattern(entry.getValue(), "201[0-9]|2020")){
                        return false;
                    }
                    break;

                case "eyr":
                    if(!matchesPattern(entry.getValue(), "202[0-9]|2030")){
                        return false;
                    }
                    break;
                case "hgt":
                    if(!matchesPattern(entry.getValue(), "1[5-8][0-9]cm|19[0-3]cm|59in|6[0-9]in|7[0-6]in")){
                        return false;
                    }
                    break;
                case "hcl":
                    if(!matchesPattern(entry.getValue(), "#[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]")){
                        return false;
                    }
                    break;
                case "ecl":
                    if(!matchesPattern(entry.getValue(), "amb|blu|brn|gry|grn|hzl|oth")){
                        return false;
                    }
                    break;
                case "pid":
                    if(!matchesPattern(entry.getValue(), "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")){
                        return false;
                    }
                    break;
            }

        }

        return true;
    }


    private boolean matchesPattern(String s, String pattern){
        Pattern p = Pattern.compile(pattern);
        boolean matches = p.matcher(s).matches();
        return matches;
    }
}