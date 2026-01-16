import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Task001 {
    public static HashMap<String, List<String>> phoneBook = new HashMap<>();

    public static void main(String[] args) {
        addInPhoneBook();
        findInPhoneBook("Иванов");
    }

    public static void addInPhoneBook() {
        phoneBook.put("Иванов", List.of("+7(000)000-00-00", "+7(111)111-11-11"));
        phoneBook.put("Петров", List.of("+7(222)222-22-22", "+7(333)333-33-33"));
        phoneBook.put("Белов", List.of("+7(444)444-44-44", "+7(555)555-55-55"));
        phoneBook.put("Светлова", List.of("+7(666)666-66-66", "+7(777)777-77-77"));
    }

    public static void findInPhoneBook(String surname) {
        System.out.printf("%s: %s", surname, phoneBook.get(surname));
    }
}