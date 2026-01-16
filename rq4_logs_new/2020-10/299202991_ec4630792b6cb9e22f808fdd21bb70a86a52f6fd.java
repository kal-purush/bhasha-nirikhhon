package seedu.ui;

import seedu.data.TaskMap;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.Locale;

public abstract class DisplayDateStructure {
    protected final TextStyle MONTH_TEXT_STYLE = TextStyle.FULL;
    protected final TextStyle WEEK_TEXT_STYLE = TextStyle.SHORT;
    protected final Locale LOCALE = Locale.ENGLISH;
    protected final int DAYS_PER_WEEK = 7;

    protected LocalDate currentDate = LocalDate.now();
    protected Month currentMonth = currentDate.getMonth();
    protected int currentDateDayOfMonth = currentDate.getDayOfMonth();
    protected DayOfWeek currentDateDayOfWeek = currentDate.getDayOfWeek();

    protected char[][] screen;

    public char[][] getScreen() {
        return screen;
    }

    protected abstract void generateScreen(TaskMap tasks);
}