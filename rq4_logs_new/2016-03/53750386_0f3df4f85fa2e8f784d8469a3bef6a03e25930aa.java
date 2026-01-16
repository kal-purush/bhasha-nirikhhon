package com.example.jennahuston.activityrecognitiontry;

import android.support.annotation.NonNull;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

public class Activity implements Comparator<Activity>, Comparable<Activity> {

    enum Type {NONE, SLEEPING, SITTING, RUNNING}

    private Type type;
    private Date start;
    private Date end;

    private final DateFormat shortDateFormat = new SimpleDateFormat("hh:mm:ss");
    private final DateFormat longDateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

    public Activity() {
        this(Type.NONE, new Date(), null);
    }

    public Activity(Type type) {
        this(type, new Date(), null);
    }

    public Activity(Type type, Date start, Date end) {
        this.type = type;
        this.start = start;
        this.end = end;
    }

    public void start() {
        this.start = new Date();
    }

    public void finish() {
        this.end = new Date();
    }

    public boolean isFinished() {
        return end != null;
    }

    public String getStartStringShort() {
        return shortDateFormat.format(start);
    }

    public String getEndStringShort() {
        return shortDateFormat.format(end);
    }

    public String getStartStringLong() {
        return longDateFormat.format(start);
    }

    public String getEndStringLong() {
        return longDateFormat.format(end);
    }

    public String getTimeRangeStringShort() {
        String s;
        if (!isFinished()) {
            s = String.format("%s", getStartStringShort());
        } else {
            s = String.format("%s - %s", getStartStringShort(), getEndStringShort());
        }
        return s;
    }

    public String toString() {
        return String.format("%s: %s", getTimeRangeStringShort(), getTypeString());
    }

    @Override
    public int compareTo(@NonNull Activity another) {
        long myStartTime = start.getTime();
        long otherStartTime = another.getStart().getTime();

        // If this started earlier,
        if (myStartTime < otherStartTime) {
            return 1;
        } else if (myStartTime > otherStartTime) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public int compare(Activity lhs, Activity rhs) {
        long lhsStartTime = lhs.getStart().getTime();
        long rhsStartTime = rhs.getStart().getTime();

        // If this started earlier,
        if (lhsStartTime < rhsStartTime) {
            return 1;
        } else if (lhsStartTime > rhsStartTime) {
            return -1;
        } else {
            return 0;
        }
    }

    public String getTypeString() {
        return type.toString();
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }
}