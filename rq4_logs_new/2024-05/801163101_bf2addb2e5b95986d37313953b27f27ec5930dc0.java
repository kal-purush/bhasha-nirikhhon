package utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.time.format.DateTimeParseException;
import java.util.Scanner;

public class Date {
    private LocalDate date;

    public Date(String dateStr) {
        DateTimeFormatter formatterFr = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.FRENCH);
        this.date = LocalDate.parse(dateStr, formatterFr);
    }

    public String formatAnglais() {
        DateTimeFormatter formatterEn = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
        return this.date.format(formatterEn);
    }

    public static String lireDateValide() {
        Scanner scanner = new Scanner(System.in);
        DateTimeFormatter formatterFr = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.FRENCH);
        LocalDate date = null;
        while (date == null) {
            System.out.print("Veuillez saisir une date au format JJ/MM/AAAA : ");
            String dateStr = scanner.nextLine();
            try {
                date = LocalDate.parse(dateStr, formatterFr);
            } catch (DateTimeParseException e) {
                System.out.println("Date invalide. Veuillez réessayer.");
            }
        }
        return date.format(formatterFr);  // Retourne la date valide saisie sous forme de chaîne de caractères
    }


}
