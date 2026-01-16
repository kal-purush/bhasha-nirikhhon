package task_9.college;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class College {
    public static void main(String[] args) throws IOException {
        List<Student> studentsList = new ArrayList<>();

        Collections.addAll(studentsList,
                new Student("Коротков Иван Петрович", 4.82),
                new Student("Краснов Денис Александрович", 3.45),
                new Student("Жарков Степан Михайлович", 4.72),
                new Student("Цветков Дмитрий Николаевич", 4.4),
                new Student("Смирнов Виталий Викторович", 3.88),
                new Student("Амосов Сергей Андреевич", 3.85));


        var reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Введите LIST для вывода информация о всех студентах и их средней оценке");
        System.out.println("Введите SORT для сортировки");
        String command = null;
        try {
            command = reader.readLine();
        } catch (IOException e) {
            e.getMessage();
        }
        if (command != null && command.equalsIgnoreCase("LIST")) {
            for (Student student : studentsList) {
                System.out.println("Студент \t" + student.getFio() + "\t Средний балл = " + student.getAverageRating());
            }
        } else if (command != null && command.equalsIgnoreCase("SORT")) {
            var reader2 = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Введите FIO если сортируем по ФИО");
            System.out.println("Введите GRADE если сортируем по среднему баллу");
            String commandSort = null;
            try {
                commandSort = reader.readLine();
            } catch (IOException e) {
                e.getMessage();
            }
            if (commandSort != null && commandSort.equalsIgnoreCase("FIO")) {
                Collections.sort(studentsList, new FIOComparator());
                System.out.println("Студенты отсортированы по ФИО:");
                System.out.println("----------------------------------------------------------------------");
                for (Student student : studentsList) {
                    System.out.println("Студент \t" + student.getFio() + "\t Средний балл = " + student.getAverageRating());
                }
            } else if (commandSort != null && commandSort.equalsIgnoreCase("GRADE")) {
                Collections.sort(studentsList, new RatingComparator());
                System.out.println("Студенты отсортированы по среднему баллу:");
                System.out.println("----------------------------------------------------------------------");
                for (Student student : studentsList) {
                    System.out.println("Студент \t" + student.getFio() + "\t Средний балл = " + student.getAverageRating());
                }
            }
            reader2.close();
        }
        reader.close();
    }
}