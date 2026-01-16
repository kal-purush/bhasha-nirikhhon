package cooper.ui;

import cooper.meetings.Meeting;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.TreeMap;

public class MeetingsUi {
    private static final String LINE = "=========================================================================";
    private static final String TABLE_TOP = "┌────────────────────────────────────────────────────────────────────┐";
    private static final String TABLE_BOT = "└────────────────────────────────────────────────────────────────────┘";

    public static void showInvalidTimeException() {
        Ui.show(LINE);
        Ui.show("The time format you entered is not accepted! Please enter again.");
        Ui.show(LINE);
    }

    public static void showDuplicateUsernameException() {
        Ui.show(LINE);
        Ui.show("The username has already been entered under that timeslot.");
        Ui.show(LINE);
    }

    public static void showCannotScheduleMeetingException() {
        Ui.show(LINE);
        Ui.show("Oops, no meeting can be scheduled!");
        Ui.show(LINE);
    }

    public static void showDuplicateMeetingException() {
        Ui.show(LINE);
        Ui.show("You have already scheduled a meeting at that time!");
        Ui.show(LINE);
    }

    public static void printAvailableCommand(String time, String username) {
        Ui.show(LINE);
        Ui.show("Success!");
        Ui.show(username + "'s availability has been added to " + time);
        Ui.show(LINE);
    }

    public static void printSuccessfulScheduleCommand(String meetingName, String time, ArrayList<String> usernames) {
        Ui.show(LINE);
        Ui.show("Success!");
        Ui.show("You have scheduled a <<" + meetingName + ">> meeting at " + time + " with "
                + listOfAvailabilities(usernames));
        Ui.show(LINE);
    }

    public static void printAvailabilities(TreeMap<LocalTime, ArrayList<String>> availability) {
        printTableHeader("Here are the availabilities:");
        for (LocalTime timing: availability.keySet()) {
            Ui.showText("│ " + timing + " │ " + listOfAvailabilities(availability.get(timing)));
        }
        Ui.show(TABLE_BOT);
        Ui.show(LINE);
    }

    private static String listOfAvailabilities(ArrayList<String> availabilities) {
        StringBuilder listOfAvailabilities = new StringBuilder();
        for (String a : availabilities) {
            /* don't need comma for last attendee */
            int indexOfLastAttendee = availabilities.size() - 1;
            if (a.equals(availabilities.get(indexOfLastAttendee))) {
                listOfAvailabilities.append(a);
            } else {
                listOfAvailabilities.append(a).append(", ");
            }
        }
        return String.valueOf(listOfAvailabilities);
    }

    private static void printTableHeader(String headerCaption) {
        Ui.show(LINE);
        Ui.show(headerCaption);
        Ui.show(TABLE_TOP);
    }

    public static void printMeetings(ArrayList<Meeting> meetingsList) {
        printTableHeader("Here are your meetings for today:");
        for (Meeting meeting : meetingsList) {
            Ui.show("│ <<" + meeting.getMeetingName() + ">> │ " + meeting.getTime() + " │ "
                    + listOfAvailabilities(meeting.getListOfAttendees()));
        }
        Ui.show(TABLE_BOT);
        Ui.show(LINE);
    }
}