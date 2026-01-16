/**
 * Created by Sanjay on 10/15/2017.
 */
public class Hour {
    public static final String AM = "AM";
    public static final String PM = "PM";

    private int militaryHour;

    public Hour(int hour) {
        if (hour < 0 || hour > 23) {
            throw new IllegalArgumentException();
        } else {
            this.militaryHour = hour;
        }
    }

    public Hour(int hour, String ampm) {
        if ((ampm != this.AM && ampm != PM) || (hour < 1 || hour > 12)) {
            throw new IllegalArgumentException();
        } else {
            this.militaryHour = (ampm == PM) ? hour + 12 : hour;
        }
    }

    public Hour(Hour other) {
        this.militaryHour = other.getHour();
    }

    public static boolean isValid(int hour) {
        return (hour >= 0 && hour <= 24);
    }

    public static boolean isValid(int hour, String ampm) {
        return ((ampm == AM || ampm == PM) && (hour >= 1 && hour <= 12));
    }

    public static void check12(int hour, String ampm) {
        if (!isValid(hour, ampm)) {
            throw new IllegalArgumentException();
        }
    }

    public static void check24(int hour) {
        if (!isValid(hour)) {
            throw new IllegalArgumentException();
        }
    }

    public static int to24HourClock(int hour, String ampm) {
        check12(hour, ampm);

        return (ampm == PM) ? hour + 12 : hour;
    }

    public static int to12HourClock(int hour) {
        check24(hour);

        return (hour > 12) ? hour - 12 : hour;
    }

    public int getHour() {
        return this.militaryHour;
    }

    public void setHour(int hour) {
        if (!isValid(hour)) {
            throw new IllegalArgumentException();
        } else {
            this.militaryHour = hour;
        }
    }

    public void setHour(int hour, String ampm) {
        if (!isValid(hour, ampm)) {
            throw new IllegalArgumentException();
        } else {
            this.militaryHour = (ampm == PM) ? hour + 12 : hour;
        }
    }

    public boolean equals(String h) {
        if (h != null && !(h.equals(""))) ;
        {
            String[] temp = h.split("\\s+");
            Integer tempHour = Integer.parseInt(temp[0]);

            return ((tempHour >= 1 && tempHour <= 12) && (temp[1] == AM || temp[1] == PM) && (tempHour == this.militaryHour));
        }
    }
}