package com.example.taxio;

import com.akshaykale.swipetimeline.TimelineObject;

public class TestO implements TimelineObject {
    String distance, url;
    Long timestamp;

    public TestO(long parseLong, String s, String image) {
        distance = s;
        timestamp = parseLong;
        url = image;
    }

    @Override
    public long getTimestamp() { return timestamp; }

    @Override
    public String getTitle() {
        return distance;
    }

    @Override
    public String getImageUrl() {
        return null;
    }
}