package Design;

import java.util.ArrayList;
import java.util.List;

public class DesignHashSet {

    static class MyHashSet {

        List<Integer> list;

        /**
         * Initialize your data structure here.
         */
        public MyHashSet() {
            list = new ArrayList<>();
        }

        public void add(int key) {
            list.add(key);
        }

        public void remove(int key) {
            // as list.remove shifts any subsequent elements to the left,
            // it has to be from the end to the start.
            for (int i = list.size() - 1; i >= 0; i--) {
                if (list.get(i) == key) {
                    list.remove(i);
                }
            }
        }

        /**
         * Returns true if this set contains the specified element
         */
        public boolean contains(int key) {
            return list.contains(key);
        }
    }
}