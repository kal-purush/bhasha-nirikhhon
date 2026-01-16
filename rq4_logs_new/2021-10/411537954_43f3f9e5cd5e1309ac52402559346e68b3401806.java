package cooper.storage;

import cooper.exceptions.InvalidFileDataException;
import cooper.meetings.MeetingManager;
import cooper.ui.Ui;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class AvailabilityStorage extends Storage {

    public AvailabilityStorage(String filePath) {
        super(filePath);
    }

    public void loadAvailability(MeetingManager cooperMeetingManager) {
        TreeMap<LocalTime, ArrayList<String>> availability = cooperMeetingManager.getAvailability();
        Scanner fileScanner = getScanner(filePath);
        readAvailability(fileScanner, availability);
    }

    public void saveAvailability(MeetingManager cooperMeetingManager) {
        try {
            writeAvailability(filePath, cooperMeetingManager.getAvailability());
        } catch (IOException e) {
            System.out.println("Error writing to file: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void readAvailability(Scanner fileScanner, TreeMap<LocalTime, ArrayList<String>> availability) {
        if (fileScanner != null) {
            while (fileScanner.hasNext()) {
                String availabilityRow = fileScanner.nextLine();
                try {
                    decodeAvailability(availabilityRow, availability);
                } catch (InvalidFileDataException e) {
                    Ui.showInvalidFileDataError();
                }
            }
        }
    }

    private static void decodeAvailability(String availabilityRowAsString, TreeMap<LocalTime, ArrayList<String>> availability)
            throws InvalidFileDataException {
        String[] availabilityRowAsArray = availabilityRowAsString.split("\\|");
        if (isInvalidFileData(availabilityRowAsArray)) {
            throw new InvalidFileDataException();
        }
        assert !isInvalidFileData(availabilityRowAsArray);

        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm");
        LocalTime availableTime = LocalTime.parse(availabilityRowAsArray[0].trim(), timeFormat);

        String[] attendeesAsArray = availabilityRowAsArray[1].trim().split(",");
        ArrayList<String> attendees = new ArrayList<>(Arrays.asList(attendeesAsArray));

        availability.put(availableTime, attendees);
    }

    private static boolean isInvalidFileData(String[] meeting) {
        if (meeting.length != 2) {
            return true;
        }

        try {
            DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm");
            LocalTime meetingTime = LocalTime.parse(meeting[0].trim(), timeFormat);
        } catch (DateTimeParseException e) {
            return true;
        }

        for (String s : meeting) {
            if (s.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static void writeAvailability(Path filePath, TreeMap<LocalTime, ArrayList<String>> meetings)
            throws IOException {
        FileWriter fileWriter = new FileWriter(filePath.toString(), false);

        for (Map.Entry<LocalTime, ArrayList<String>> e : meetings.entrySet()) {
            String encodeAvailability = encodeAvailability(e);
            fileWriter.write(encodeAvailability + System.lineSeparator());
        }
        fileWriter.close();
    }

    private static String encodeAvailability(Map.Entry<LocalTime, ArrayList<String>> meeting) {
        StringBuilder encodedAvailability = new StringBuilder();

        String availableTime = meeting.getKey().toString();
        encodedAvailability.append(availableTime).append(" | ");

        String availabilities = getAttendeesAsString(meeting.getValue());
        encodedAvailability.append(availabilities);

        return String.valueOf(encodedAvailability);
    }

    private static String getAttendeesAsString(ArrayList<String> attendees) {
        StringBuilder attendeesAsString = new StringBuilder();
        for (String a : attendees) {
            /* don't need comma for last attendee */
            int indexOfLastAttendee = attendees.size() - 1;
            if (a.equals(attendees.get(indexOfLastAttendee))) {
                attendeesAsString.append(a);
            } else {
                attendeesAsString.append(a).append(",");
            }
        }
        return String.valueOf(attendeesAsString);
    }
}