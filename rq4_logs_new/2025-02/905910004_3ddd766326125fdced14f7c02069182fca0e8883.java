package lesson_14_Collections_Framework.livecoding;

import java.util.*;

public class MyMainExp {
    public static void main(String[] args) {

        List<Integer> arrayList = new ArrayList<>();
        List<Integer> linkedList = new LinkedList<>();
        List<Integer> vectorList = new Vector<>();
        List<Integer> stackList = new Stack<>();

        Set<Integer> hashSet = new HashSet<>();
        Set<Integer> linkedHashSet = new LinkedHashSet<>();
        Set<Integer> treeSet = new TreeSet<>();

        Queue<Integer> priorityQueue = new PriorityQueue<>();
        Queue<Integer> queue = new ArrayDeque<>();

        Map<Integer, String> hashMap = new HashMap<>();
        Map<Integer, String> treeMap = new TreeMap<>();
        Map<Integer,String> linkedHashMap = new LinkedHashMap<>();

        // null key forbidden, hashtable is synchronized (thats why its slow).
        Map<Integer, String> hashtable = new Hashtable<>();


        arrayList.add(10);
        arrayList.add(200);
        arrayList.add(3_000);
        arrayList.add(40_000);
        arrayList.add(500_000);
        arrayList.add(null);
        System.out.println(arrayList.contains(10));
        System.out.println(arrayList.indexOf(3000));
        System.out.println(arrayList.get(4));

        linkedList.addFirst(99); // O(1)
        linkedList.addLast(213); // O(1)
    }
}