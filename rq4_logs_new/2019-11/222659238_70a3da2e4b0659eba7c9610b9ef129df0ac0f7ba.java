package Collections;

import java.util.*;

public class ClassNoteBookMap {
    private Map<String, List<String>> noteBook = new HashMap<String, List<String>>();

    public ClassNoteBookMap() {
    }

    void add(String name, String telephone){

        List<String> ls = new ArrayList<>();
        List<String> find_element = noteBook.getOrDefault(name, ls);

        find_element.add(telephone);
       noteBook.put(name, find_element);

    }

    List<String> get(String name){

        return  noteBook.getOrDefault(name, Arrays.asList("Не обнаружены"));

    }

}