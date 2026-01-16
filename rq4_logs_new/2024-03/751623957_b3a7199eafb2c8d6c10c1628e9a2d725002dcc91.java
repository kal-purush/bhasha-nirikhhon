package com;

public static class Attendee {
    private String name;
    private boolean checkedIn;
    private int checkInCount;
    private String profilePicture;

    public Attendee(String name) {
        this.name = name;
        this.checkedIn = false;
        this.checkInCount = 0;
        this.profilePicture = null;
    }

    public void checkIn() {
        this.checkedIn = true;
        this.checkInCount++;
    }

    public boolean isCheckedIn() {
        return checkedIn;
    }

    public int getCheckInCount() {
        return checkInCount;
    }

    public String getName() {
        return name;
    }

    public String getProfilePicture() {
        return profilePicture;
    }

    public void setProfilePicture(String profilePicture) {
        this.profilePicture = profilePicture;
    }
}