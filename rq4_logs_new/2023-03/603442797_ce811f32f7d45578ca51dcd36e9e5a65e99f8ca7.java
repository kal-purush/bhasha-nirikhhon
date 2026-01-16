package homework.day7.stringtask;

import java.time.LocalDate;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class GenDate {

    public void GenerationDate(String text) {

        char[] wordCharacters = text.toLowerCase().toCharArray();
        char[] neededCharacters = new char[]{'а', 'е', 'ё', 'и', 'о', 'у', 'ы', 'э', 'ю', 'я', 'a', 'e', 'i', 'o', 'u', 'y'};
        int neededCounter = 0;
        for (int i = 0; i < wordCharacters.length; i++) {
            for (int j = 0; j < neededCharacters.length; j++) {
                if (wordCharacters[i] == neededCharacters[j]) {
                    neededCounter++;
                }
            }
        }
        LocalDate currentDate = LocalDate.now();
        int dayOfMonth = currentDate.getDayOfMonth();
        int month = currentDate.getMonthValue();
        int year = currentDate.getYear();
        Calendar calendar = new GregorianCalendar(year, (month - 1), dayOfMonth);
        calendar.add(Calendar.DAY_OF_MONTH, neededCounter);
        System.out.printf("Сгенерированная гласная дата: %1$te %1$tB", calendar.getTime()).println();
    }
}