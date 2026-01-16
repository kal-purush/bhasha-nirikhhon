package com.metafour.cwbay.model;

import java.util.Date;

/**
 * Created by nadim on 1/27/15.
 */
public class Ad {
    public static final String JSON_TAG_ID = "id";
    public static final String JSON_TAG_TITLE = "title";
    public static final String JSON_TAG_PRICE = "price";
    public static final String JSON_TAG_POSTED_AT = "postedAt";
    public static final String JSON_TAG_POSTED_FROM = "postedFrom";

    private int id;
    private String title;
    private double price;
    private Date postedAt;
    private String postedFrom;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Date getPostedAt() {
        return postedAt;
    }

    public void setPostedAt(Date postedAt) {
        this.postedAt = postedAt;
    }

    public String getPostedFrom() {
        return postedFrom;
    }

    public void setPostedFrom(String postedFrom) {
        this.postedFrom = postedFrom;
    }

    @Override
    public String toString() {
        return "Ad{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", price=" + price +
                ", postedAt=" + postedAt +
                ", postedFrom='" + postedFrom + '\'' +
                '}';
    }
}