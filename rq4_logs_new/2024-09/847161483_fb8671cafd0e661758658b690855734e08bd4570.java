package Class;
import java.util.*;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;

public class DateTimeFormat {
    public static void main(String[] args) {
        DateTimeFormatter one= DateTimeFormatter.ofPattern("yyyy/MM/dd") ;
        LocalDate date= LocalDate.now();
       String result=date.format(one);
        System.out.println(result);

    }
}