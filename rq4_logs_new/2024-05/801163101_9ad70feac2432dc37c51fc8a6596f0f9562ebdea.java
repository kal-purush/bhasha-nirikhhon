package utils;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateTimeOperations {
    private static final String FILE_NAME = "datetime.txt";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static void saveCurrentDateTime() {
        LocalDateTime currentDateTime = LocalDateTime.now();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_NAME))) {
            writer.write(currentDateTime.format(FORMATTER));
            System.out.println("Successfully saved current date and time: " + currentDateTime);
        } catch (IOException e) {
            System.err.println("An error occurred while writing to the file: " + e.getMessage());
        }
    }

    public static LocalDateTime readSavedDateTime() {
        LocalDateTime savedDateTime = null;

        try (BufferedReader reader = new BufferedReader(new FileReader(FILE_NAME))) {
            String dateTimeString = reader.readLine();
            if (dateTimeString != null) {
                savedDateTime = LocalDateTime.parse(dateTimeString, FORMATTER);
                System.out.println("Successfully read saved date and time: " + savedDateTime);
            } else {
                System.err.println("The file is empty.");
                return LocalDateTime.now();
            }
        } catch (IOException e) {
            System.err.println("An error occurred while reading from the file: " + e.getMessage());
        }

        return savedDateTime;
    }

    public static boolean is15MinutesElapsed(LocalDateTime savedDateTime) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        return java.time.Duration.between(savedDateTime, currentDateTime).toMinutes() >= 15;
    }

    public static void main(String[] args) {
        // Enregistrer la date et l'heure actuelles
        saveCurrentDateTime();

        // Lire la date et l'heure enregistrées
        LocalDateTime savedDateTime = readSavedDateTime();

        // Vérifier si 15 minutes se sont écoulées
        if (savedDateTime != null) {
            boolean elapsed = is15MinutesElapsed(savedDateTime);
            System.out.println("Has 15 minutes elapsed since saved time? " + elapsed);
        }
    }
}