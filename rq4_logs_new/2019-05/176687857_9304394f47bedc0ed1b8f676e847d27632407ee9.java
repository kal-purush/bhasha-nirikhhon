package it.polito.mad.appcomplete;

import java.util.Comparator;

public class Comment {
    private String date;
    private String title;
    private Float stars;
    private String description;

    public Comment(String date, String title, Float star, String description) {
        this.date = date;
        this.title = title;
        this.stars = star;
        this.description = description;
    }

    public Comment() {

    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Float getStars() {
        return stars;
    }

    public void setStars(Float star) {
        this.stars = star;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public static final Comparator<Comment> BY_STAR_DESCENDING = new Comparator<Comment>() {
        @Override
        public int compare(Comment c1, Comment c2) {
            return c2.getStars().compareTo(c1.getStars());
        }
    };
}