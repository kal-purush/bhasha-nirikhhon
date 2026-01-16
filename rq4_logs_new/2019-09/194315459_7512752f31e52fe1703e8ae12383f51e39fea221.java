package com.lern.homeWork.lesson5.regularExpresion;

/* Задан log-file с описанием четырёхдневных курсов. Каждая строка начинается со времени,
за которым следует описание активности. Пустые строки разделяют дни. Некоторые активности
представляют собой названия лекций, например "Введение", "Скаляры", "Массивы". Другие -
названия определённых повторяющихся отрезков времени: "Упражнения", "Перерыв", "Решения" и т.д.
Словом "Конец" отмечается конец дня. Задание: Используя регулярные выражения считать данные
из String, а затем сгенерировать два отчёта в двух разных файлах в следующем виде:
1. В виде временных отрезков: 09:20-11:00 Введение 11:00-11:15 Упражнения 11:15-11:35 Перерыв
2. В виде общего времени, потраченного на активности за день, и детализированного описания лекций:
Лекции: 210 минут 22% Решения: 95 минут 9% Перерыв: 65 минут 6% . Лекции: Введение:  23 минуты 2% */

import java.lang.reflect.Array;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegularCourse {
    public static void main(String[] args) {

        final String logFile = "09:20 Введение" + "\n" +
                "11:00 Упражнения" + "\n" +
                "11:15 Перерыв" + "\n" +
                "11:35 Скаляры" + "\n" +
                "12:30 Обеденный перерыв" + "\n" +
                "13:30 Упражнения" + "\n" +
                "14:10 Решения" + "\n" +
                "14:30 Перерыв" + "\n" +
                "14:40 Массивы" + "\n" +
                "15:40 Упражнения" + "\n" +
                "17:00 Решения" + "\n" +
                "17:30 Конец" + "\n" +
                "\n" +
                "09:30 Углубленное изучение массивов" + "\n" +
                "10:30 Перерыв" + "\n" +
                "10:50 Упражнения";

        Pattern pattern = Pattern.compile("[0-1][\\d]:[0-5][0|5]");

        Matcher matcher = pattern.matcher(logFile);

        while (matcher.find()) {
            System.out.print("Start index: " + matcher.start()); // matcher.end() - end index = matcher.start() + 5
            System.out.println(" " + matcher.group());
        }

        String time[] = logFile.split("\\s+[A-ZА-Яa-zа-я]+");
        for (int i = 0; i < time.length; i++) {
            System.out.print(time[i] + " ");
        }
        System.out.println();

        String words[] = logFile.split("[0-1][\\d]:[0-5][0|5]\\s+");
        for (int i = 0; i < words.length; i++) {
            System.out.print(words[i] + " ");
        }
        System.out.println();

        for (int i = 0, j = i + 1, k = 0; i < time.length - 1 && j < time.length && k < words.length; i++, j++, k++) {
            System.out.println(time[i] + "-" + time[j] + " " + words[k]);
        }
    }
}