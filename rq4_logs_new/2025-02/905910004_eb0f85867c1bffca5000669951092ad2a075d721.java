package lesson_14_Collections_Framework.bonustask;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DataHandler {

    public String formOutput(List<String> list, int index) {
        try {
            String name = list.get(index);
            return name + " is in index " + index;
        } catch (IndexOutOfBoundsException e) {
            return "Wrong index!";
        }
    }

    public String formListOutput(List<String> list) {
        StringBuilder sb = new StringBuilder();
        AtomicInteger count = new AtomicInteger(1);
        for (String name : list) {
            sb.append(String.format("(%d) %s", count.getAndIncrement(), name));
        }
        return sb.toString();
    }
}