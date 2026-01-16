package fr.wildcodeschool.cafeconcert;

public class Bar {

    private String barName;
    private String phoneNumber;
    private double geoShape;
    private double geoPoint;
    private String webUrl;

    private boolean isLiked; // null if neutral, true if liked, false if disliked


    public Bar(String barName, String phoneNumber, double geoShape, double geoPoint, String webUrl, boolean isLiked) {
        this.barName = barName;
        this.phoneNumber = phoneNumber;
        this.geoShape = geoShape;
        this.geoPoint = geoPoint;
        this.webUrl = webUrl;
        this.isLiked = isLiked;
    }

    public String getBarName() {
        return barName;
    }

    public void setBarName(String name) {
        this.barName = name;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public double getGeoShape() {
        return geoShape;
    }

    public void setGeoShape(double geoShape) {
        this.geoShape = geoShape;
    }

    public double getGeoPoint() {
        return geoPoint;
    }

    public void setGeoPoint(double geoPoint) {
        this.geoPoint = geoPoint;
    }

    public String getWebUrl() {
        return webUrl;
    }

    public void setWebUrl(String webUrl) {
        this.webUrl = webUrl;
    }

    public boolean isLiked() {
        return isLiked;
    }

    public void setLiked(boolean liked) {
        isLiked = liked;
    }


}