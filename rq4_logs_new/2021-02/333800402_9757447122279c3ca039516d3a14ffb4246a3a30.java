package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson3.lection3;
import java.util.*;
import java.util.function.Predicate;

public class Test {
    public static void main(String ... args) {
        /*ArrayList<Integer> array = new ArrayList<>();
        array.add(1);
        array.add(22);
        array.add(4);
        array.add(2, 3);
        ArrayList<Integer> array2 = new ArrayList<>();
        array2.add(1);
        array2.add(22);
        // Predicate<Integer> lessThanFive = x -> x<5;
        // array.removeIf(x -> x<5);
        System.out.println(array);
        String [] words = {"one", "two", "three", "four"};
        List<String> listString = Arrays.asList(words);//ne menyaetsya razmer
        System.out.println(listString);
        listString.set(0, "Un");
        listString.replaceAll(s -> s.toUpperCase());
        //listString.add("string");
        System.out.println(listString);
        ArrayList <String> arrayListString = new ArrayList<>(Arrays.asList(words));
        arrayListString.add("one more");
        arrayListString.remove(0);
        System.out.println(arrayListString);*/
        ArrayList <String> arrayListString2 = new ArrayList<>(Arrays.asList("1", "2", "3"));
       /* for(String s: arrayListString2) {
            System.out.println(s);
        }
        arrayListString2.get(0);
        LinkedList<String> linkedList = new LinkedList<>();
        linkedList.add("one");
        Set<String> hashSet = new HashSet<>(arrayListString2);
        hashSet.add("2");
        hashSet.add("15");
        hashSet.add("4");
        System.out.println(hashSet);
        Set<String> set = Set.of("one", "two", "three", "four", "2", "15", "7"); // nelzya menyat razmer
        hashSet.addAll(set);
        hashSet.add("five");
        System.out.println(hashSet);
        String [] words = {"one", "two", "three", "four", "five"};
        TreeSet <Student> treeSet = new TreeSet<>(Comparator.reverseOrder());
        treeSet.add(new Student(21, "Petr"));
        treeSet.add(new Student(19, "Anne"));
        treeSet.add(new Student(20, "Anne"));
        treeSet.add(new Student(19, "John"));
        System.out.println(treeSet);
        TreeSet <String> treeSetReverse = new TreeSet<>(Comparator.reverseOrder());
        treeSetReverse.add("a");
        treeSetReverse.add("b");
        treeSetReverse.add("x");
        treeSetReverse.add("l");
        System.out.println(treeSetReverse);

        Map<String, List<String>> map = new HashMap<>();
        Map<String, List<String>> map = new TreeMap<>();
        map.put("one", new ArrayList<>(Arrays.asList("1", "2", "3")));
        map.put("two", Arrays.asList("1", "2", "3"));
        map.put("three", Arrays.asList("1", "2", "3"));
        map.put("two", Arrays.asList("1", "2", "3"));
        if(map.containsKey("one")) {
            map.get("one").add("0");
        }

        System.out.println(map);*/
//        Map<String, String> map1 = Map.ofEntries(
//                Map.entry("a", "b"),
//                Map.entry("x", "y")
//        );
//        Iterator <Map.Entry<String, String>> it = map1.entrySet().iterator();
//        while(it.hasNext()){
//            Map.Entry<String, String> entry = it.next();
//            System.out.println(entry.getKey());
//        }
//
//        for(Map.Entry<String, String> entry: map1.entrySet()) {
//            System.out.println(entry.getKey()+" : "+entry.getValue());
//        }

    }
}

class Student implements Comparable<Student>{
    String name;
    Integer age;

    public Student(Integer age, String name) {
        this.age = age;
        this.name = name;
    }

    @Override
    public int compareTo(Student o) {
        int result = this.name.compareTo(o.name);
        if(result == 0) {
            return o.age - this.age;
        }
        return result;
        //return this.name.compareTo(o.name); //vizivaet compareTo String
        //return o.age - this.age;
        /* esli ravni, to 0
        esli nash bolshe, to chislo > 0, libo 1,
        esli student o bolshe, to chislo < 0, libo -1
         */
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}