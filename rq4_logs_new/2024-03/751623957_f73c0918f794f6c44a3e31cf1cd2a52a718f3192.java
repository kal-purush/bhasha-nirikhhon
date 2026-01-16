package com.example.its_a_feature_not_a_bug;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Date;

public class EventUnitTest {
    private Event mockEvent;

    @Test
    public void testGetImageId() {
        String imageId = "https://image1.png";
        mockEvent = new Event(imageId, "Study Group", "Hindle", null, "This is a test.", 5);
        assertEquals(imageId, mockEvent.getImageId());
    }

    @Test
    public void testSetImageId() {
        mockEvent = new Event("https://image1.png", "Study Group", "Hindle", null, "This is a test.", 5);
        String newImageId = "https://image2.png";
        mockEvent.setImageId(newImageId);
        assertEquals(newImageId, mockEvent.getImageId());
    }

    @Test
    public void testGetTitle() {
        String title = "Study Group";
        mockEvent = new Event("https://image1.png", title, "Hindle", null, "This is a test.", 5);
        assertEquals(title, mockEvent.getTitle());
    }

    @Test
    public void testSetTitle() {
        mockEvent = new Event("https://image1.png", "Study Group", "Hindle", null, "This is a test.", 5);
        String newTitle = "Study Group 2";
        mockEvent.setTitle(newTitle);
        assertEquals(newTitle, mockEvent.getTitle());
    }

    @Test
    public void testGetHost() {
        String host = "Hindle";
        mockEvent = new Event("https://image1.png", "Study Group", host, null, "This is a test.", 5);
        assertEquals(host, mockEvent.getHost());
    }

    @Test
    public void testSetHost() {
        mockEvent = new Event("https://image1.png", "Study Group", "Hindle", null, "This is a test.", 5);
        String newHost = "Hazel";
        mockEvent.setHost(newHost);
        assertEquals(newHost, mockEvent.getHost());
    }

    @Test
    public void testGetDescription() {
        String desc = "This is a test.";
        mockEvent = new Event("https://image1.png", "Study Group", "Hindle", null, desc, 5);
        assertEquals(desc, mockEvent.getDescription());
    }

    @Test
    public void testSetDescription() {
        mockEvent = new Event("https://image1.png", "Study Group", "Hindle", null, "This is a test.", 5);
        String newDesc = "This is not a test.";
        mockEvent.setDescription(newDesc);
        assertEquals(newDesc, mockEvent.getDescription());
    }
}