package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson3.hw3;
import java.util.HashMap;
import java.util.Map;
//Задание 2
public class CreateBase {
    public static void main(String[] args){
        System.out.println(PhoneDirectory.add("Копылов",1));
        System.out.println(PhoneDirectory.add("Петров",2));
        System.out.println(PhoneDirectory.add("Иванов",3));
        System.out.println(PhoneDirectory.add("Иванов",5));
        System.out.println(PhoneDirectory.add("Копылов",4));
       PhoneDirectory.get("Иванов");
        PhoneDirectory.get("Копылов");
        //PhoneDirectory.printMap();
    }
}