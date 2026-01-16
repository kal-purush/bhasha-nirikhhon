package homework.friday;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BlackFridayService {
    public static void printBlackFridayPerYearSorted(int startYear, int endYear) {
        if (startYear < 0 || endYear < startYear) {
            throw new IllegalArgumentException();
        }

        LocalDate startDate = LocalDate.of(startYear, 1, 1);
        LocalDate endDate = LocalDate.of(endYear, 1, 1);

        Map<Integer, Long> year2BlackFridayNumber = Stream.iterate(startDate, date -> date.plusDays(1))
                .filter(date -> date.getDayOfMonth() == 13)
                .filter(date -> date.getDayOfWeek() == DayOfWeek.FRIDAY)
                .takeWhile(endDate::isAfter)
                .collect(Collectors.groupingBy(LocalDate::getYear, Collectors.counting()));

        year2BlackFridayNumber.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .forEach(System.out::println);
    }

    public static void main(String[] args) {
        printBlackFridayPerYearSorted(2015, 2021);
    }
}