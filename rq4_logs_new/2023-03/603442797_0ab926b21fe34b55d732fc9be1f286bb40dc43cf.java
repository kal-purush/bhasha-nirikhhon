package homework.day7.stringtask;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class DateFormattingSt {

        public static void Formatting(String FirstDate) {
            Locale usLocale = new Locale("en", "US");
            LocalDateTime date = LocalDateTime.parse(FirstDate, DateTimeFormatter.ofPattern("HH.mm dd.MM.yyyy"));
            System.out.println(date.format(DateTimeFormatter.ofPattern("MMMM, d, yyyy HH:mm", usLocale)));

        }
}