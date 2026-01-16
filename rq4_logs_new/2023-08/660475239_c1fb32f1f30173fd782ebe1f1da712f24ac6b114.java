package day17methodoverloadingdatetimeclasses;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTime01 {
    public static void main(String[] args) {

        Date current = new Date();
        System.out.println(current);
        System.out.println(current.getTime());

        LocalDateTime myDate = LocalDateTime.now();
        System.out.println(myDate);

        System.out.println(LocalDateTime.now(ZoneId.of("Asia/Baku")));
        System.out.println(LocalDateTime.now(ZoneId.of("Europe/Istanbul")));

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd:MMMM:yyyy");
        String formatted = dtf.format(myDate);
        System.out.println(formatted);


    }
}