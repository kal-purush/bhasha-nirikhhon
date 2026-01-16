import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

class GroupAnagrams {
    public List<List<String>> groupAnagrams(String[] strs) {
        List<List<String>> result = new ArrayList<>();
        HashMap<String, List<String>> hashMap = new HashMap<>();
        for(String str: strs) hashMap.computeIfAbsent(sortedString(str), key -> new ArrayList<>()).add(str);
        for(List<String> value: hashMap.values()) result.add(value);
        return result;
    }

    private String sortedString(String str) {
        char[] strArray = str.toCharArray();
        Arrays.sort(strArray);
        return new String(strArray);
    }

    public static void main(String[] args) {
        GroupAnagrams groupAnagrams = new GroupAnagrams();
        System.out.println(groupAnagrams.groupAnagrams(new String[]{"eat","tea","tan","ate","nat","bat"}));
    }
}