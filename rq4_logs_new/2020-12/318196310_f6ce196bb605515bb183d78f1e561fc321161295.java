package be.upgrade.it.adventofcode2020.day7;

import java.util.*;
import java.util.stream.Collectors;

public class LuggageRule {
    public static final String BAGS_CONTAIN = " bags contain ";
    public static final String NO_OTHER_BAGS = "no other bags.";

    private final String color;
    private final List<ContainingBag> containingBags;

    public LuggageRule(String rule) {
        String[] split = rule.split(BAGS_CONTAIN);
        this.color = split[0].trim();


        this.containingBags = Arrays.stream(split[1].trim().split(","))
            .map(
                s -> {
                    if(NO_OTHER_BAGS.equals(s)){
                        return null;
                    }
                    String[] parts = s.trim().split(" ");
                    return new ContainingBag(Integer.parseInt(parts[0]), parts[1] + " " + parts[2]);
                }
            )
            .filter(r -> r != null)
            .collect(Collectors.toList());
    }



    public static int getNumberOfOuterBagsForColor(String[] input, String color){
        List<LuggageRule> rules = Arrays.stream(input).map(LuggageRule::new).collect(Collectors.toList());
        Set<String> matches = getContainingBagColors(color, rules);

        return matches.size();
    }

    private static Set<String> getContainingBagColors(String color, Collection<LuggageRule> rules){
        Set<String> directMatches = rules
                .stream()
                .filter(r -> {
                    for (ContainingBag c : r.containingBags) {
                        if (c.color.equals(color)) {
                            return true;
                        }
                    }
                    return false;
                })
                .map(r -> r.color).collect(Collectors.toSet());
        if(directMatches.isEmpty()){
            return directMatches;
        } else {
            Set<String> allMatches = new HashSet<>();
            allMatches.addAll(directMatches);
            for (String directMatch : directMatches) {
                allMatches.addAll(getContainingBagColors(directMatch, rules));
            }
            return allMatches;
        }
    }

    public static int getTotalNumberOfBagsForColor(String[] input, String color){
        Map<String, LuggageRule> rulesByColor = Arrays.stream(input).map(LuggageRule::new).collect(Collectors.toMap(r -> r.color, r -> r));

        int count = getNumberOfBagsForColor(color, rulesByColor);

        return count-1;


    }

    private static int getNumberOfBagsForColor(String color, Map<String, LuggageRule> rulesByColor) {
        LuggageRule luggageRule = rulesByColor.get(color);
        if(luggageRule == null){
            return 1;
        }
        if(luggageRule.containingBags.size() == 0){
            return 1;
        }
        int count = 1;
        for (ContainingBag containingBag : luggageRule.containingBags) {
            int numberOfContainingBagsForColor = getNumberOfBagsForColor(containingBag.color, rulesByColor);
            count += (numberOfContainingBagsForColor * containingBag.amount);
        }
        return count;

    }


    private static class ContainingBag{
        private final int amount;
        private final String color;

        public ContainingBag(int amount, String color) {
            this.amount = amount;
            this.color = color;
        }

        public int getAmount() {
            return amount;
        }

        public String getColor() {
            return color;
        }
    }
}
