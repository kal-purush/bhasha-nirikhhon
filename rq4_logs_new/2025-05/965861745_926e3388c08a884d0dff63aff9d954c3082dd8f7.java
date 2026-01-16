package com.daclink.citypulse.model;

import com.google.gson.annotations.SerializedName;

public class EventItem {
    @SerializedName("name")
    private String name;

    @SerializedName("dates")
    private Dates dates;

    @SerializedName("_embedded")
    private EmbeddedVenues embeddedVenues;

    public String getName() {
        return name;
    }

    public String getLocalDate() {
        if (dates != null && dates.start != null) {
            return dates.start.localDate + " " + dates.start.localTime;
        }
        return "No date";
    }

    public String getVenueName() {
        if (embeddedVenues != null && embeddedVenues.venues != null && !embeddedVenues.venues.isEmpty()) {
            return embeddedVenues.venues.get(0).name;
        }
        return "Unknown venue";
    }

    private static class Dates {
        Start start;

        private static class Start {
            @SerializedName("localDate")
            String localDate;

            @SerializedName("localTime")
            String localTime;
        }
    }

    private static class EmbeddedVenues {
        @SerializedName("venues")
        java.util.List<Venue> venues;

        private static class Venue {
            @SerializedName("name")
            String name;
        }
    }
}