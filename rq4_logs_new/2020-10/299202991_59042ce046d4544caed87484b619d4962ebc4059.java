package seedu.planus;

import java.io.InputStream;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Scanner;

public class Ui {
    //    private final int DISPLAY_LENGTH = 140;
    //    private final int DISPLAY_HEIGHT = 50;

    //    private char[][] screen = new char[DISPLAY_HEIGHT][DISPLAY_LENGTH];
    private char[][] screen = null;
    private String message = null;
    private final Scanner in;
    private final PrintStream out;
    private ArrayList<Integer> tasks;

    private Month month = LocalDateTime.now().getMonth();

    public Ui(ArrayList<Integer> tasks) {
        this(System.in, System.out, tasks);
    }

    private Ui(InputStream in, PrintStream out, ArrayList<Integer> tasks) {
        this.in = new Scanner(in);
        this.out = out;
        this.tasks = tasks;
    }

    public String getUserInput() {
        return in.nextLine();
    }

    private void displayAllByTime() {
        // Sort by datetime
    }

    private void displayAllByPriority() {
        // Sort by priority, same priority then datetime
    }

    private void generateScreenWeeklyView() {
        screen = new char[21][120];
        for (int i = 0; i <= 20; i++) {
            for (int j = 0; j <= 119; j++) {
                if (i % 20 == 0 || j % 17 == 0) {
                    screen[i][j] = '*';
                } else {
                    screen[i][j] = ' ';
                }
            }
        }
    }

    public void printScreen() {
        generateScreenWeeklyView();
        for (char[] arr : screen) {
            out.println(arr);
        }
    }

    public Month getMonth() {
        return month;
    }

    public void setMonth(Month month) {
        this.month = month;
    }

    public char[][] getScreen() {
        return screen;
    }

    public void setScreen(char[][] screen) {
        this.screen = screen;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}