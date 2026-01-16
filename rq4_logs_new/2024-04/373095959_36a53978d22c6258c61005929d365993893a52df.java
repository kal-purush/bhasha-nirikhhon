package org.frank.leetcode.array.count.items.matching.a.rule.demo02;

import java.util.List;

public class CountItemsMatchingARuleSolution2 {
    public int countMatches(List<List<String>> items, String ruleKey, String ruleValue) {
        int index = 0;
        switch (ruleKey){
            case "type" :
                index = 0;
                break;
            case "color" :
                index = 1;
                break;
            case "name" :
                index = 2;
                break;              
        }
        int count = 0;
        for(List<String> item: items){
            if(item.get(index).equals(ruleValue)){
                count ++;
            }
        }
        return count;
    }    
}