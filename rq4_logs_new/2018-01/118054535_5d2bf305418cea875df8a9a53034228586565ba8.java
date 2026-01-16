package ca.ualberta.cs.lonelytwitter;

import java.time.Instant;
import java.util.Date;

public abstract class Mood {

    private Date date;

    public Mood() {
        this.date = new Date();
    }

    public Mood(Date date) {
        this.date = date;
    }

    public Date getDate() {
        return date;
    }

    public abstract String format();
}