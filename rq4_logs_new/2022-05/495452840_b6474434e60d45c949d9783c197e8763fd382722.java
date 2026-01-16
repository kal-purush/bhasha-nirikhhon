import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

class Solution {
    public static void main(String[] argh) {
        Scanner scan = new Scanner(System.in);
        int n = scan.nextInt();
        Map<String, Integer> phoneBook = new HashMap<String, Integer>();

        for (int i = 0; i < n; i++) {
            String name = scan.next();
            int phone = scan.nextInt();
            phoneBook.put(name, phone);
        }
        while (scan.hasNext()) {
            String s = scan.next();
            if (phoneBook.containsKey(s)) {
                System.out.println(s + "=" + phoneBook.get(s));
            } else {
                System.out.println("Not found");
            }
        }

    }
}