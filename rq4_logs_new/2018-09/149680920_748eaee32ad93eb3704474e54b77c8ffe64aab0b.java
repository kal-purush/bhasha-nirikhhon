package ca.ualberta.cs.lonelytwitter;

import java.util.Date;

public abstract class Emotion {

    private Date date;

    public Emotion() {
        this.setDate(new Date(0));
    }

    public Emotion(Date date) {
        this.setDate(date);
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Date getDate() {
        return date;
    }

    public abstract String emotionDescription();
}