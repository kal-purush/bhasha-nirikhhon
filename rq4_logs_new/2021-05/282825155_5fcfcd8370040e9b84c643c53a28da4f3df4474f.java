package ru.job4j.condition;

public class MultipleSwitchWeek {
    public static int numberOfDay(String name) {
        return switch (name) {
            case "Monday", "Понедельник" -> 1;
            case "Tuesday", "Вторник" -> 2;
            case "Wednesday", "Среда" -> 3;
            case "Thursday", "Четверг" -> 4;
            case "Friday", "Пятница" -> 5;
            case "Saturday", "Суббота" -> 6;
            case "Sunday", "Воскресенье" -> 7;
            default -> -1;
        };
    }
}