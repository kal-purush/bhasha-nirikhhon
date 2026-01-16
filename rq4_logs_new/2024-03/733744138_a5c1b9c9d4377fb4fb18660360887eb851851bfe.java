package br.dev.ferreiras.challenges.leetCode;

import java.util.LinkedHashMap;
import java.util.Map;

/*
You are given two strings order and s. All the characters of order are unique and were sorted
in some custom order previously.

Permute the characters of s so that they match the order that order was sorted.
More specifically, if a character x occurs before a character y in order, then x should occur
before y in the permuted string.

Return any permutation of s that satisfies this property.

Example 1:

Input:  order = "cba", s = "abcd"

Output:  "cbad"

Explanation:
"a", "b", "c" appear in order, so the order of "a", "b", "c" should be "c", "b", and "a".

Since "d" does not appear in order, it can be at any position in the returned string.
"dcba", "cdba", "cbda" are also valid outputs.

Example 2:

Input:  order = "bcafg", s = "abcd"

Output:  "bcad"

Explanation:
The characters "b", "c", and "a" from order dictate the order for the characters in s.
The character "d" in s does not appear in order, so its position is flexible.

Following the order of appearance in order, "b", "c", and "a" from s should be arranged
as "b", "c", "a". "d" can be placed at any position since it's not in order.
The output "bcad" correctly follows this rule.
Other arrangements like "bacd" or "bcda" would also be valid, as long as "b", "c", "a"
maintain their order.

Constraints:

1 <= order.length <= 26
1 <= s.length <= 200
order and s consist of lowercase English letters.
All the characters of order are unique.
 */
public class CustomOrderString {
  public static void main(String[] args) {
    String order = "bcafg";
    String s = "abcd";

    String r = customSortString(order, s);
    System.out.println(r);
  }

  public static String customSortString(String order, String s) {
    String string = "";
    int size = order.length();
    Map<Character, Integer> map = new LinkedHashMap<>();

    for (int i = 0; i < s.length(); i++) {
//      if(!map.containsKey(s.charAt(i))) {
//        map.put(s.charAt(i), 1);
//      } else {
//        map.put(s.charAt(i), map.getOrDefault((s.charAt(i)),0) + 1);
//      }
      map.put(s.charAt(i), map.getOrDefault((s.charAt(i)), 0) + 1);
    }

    StringBuilder sb = new StringBuilder();
    for (char key : order.toCharArray()) {
      if (map.containsKey(key)) {
        sb.append(String.valueOf(key).repeat(Math.max(0, map.get(key))));
      }
      map.put(key, 0);
    }
    for (Map.Entry<Character, Integer> entry : map.entrySet()) {
      char key = entry.getKey();
      int value = entry.getValue();
      if (value != 0) {
        for (int j = 0; j < value; j++) {
          sb.append(key);
        }
      }
    }
    System.out.println(map);
    System.out.println(sb.toString());

    return string;
  }
}