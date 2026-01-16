import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

public class Main {

    public static void main(String[] args) {

        Scanner scn = null;
        ArrayList<Integer> forest = new ArrayList<>();
        HashMap<Integer, Integer> forester = new HashMap<>();
        String tree;

        try {
            scn = new Scanner(new FileInputStream("data.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        while (scn.hasNext()) {
            tree = scn.next();
            if (!tree.isEmpty()) {
                forest.add(Integer.parseInt(tree));
            }
        }

        for (Integer i : forest) {
            if (!forester.containsKey(i)) {
                forester.put(i, 1);
            } else {
                forester.put(i, (forester.get(i) + 1));
            }
        }

        System.out.println("The forest contains");
        Set<Integer> keySet = new HashSet(forester.keySet());
        for (Integer i : keySet) {
            System.out.println(forester.get(i) + " trees of type " + i);
        }

    }
}