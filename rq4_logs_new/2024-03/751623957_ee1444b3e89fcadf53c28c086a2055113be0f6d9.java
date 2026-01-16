package com.example.its_a_feature_not_a_bug;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import com.example.its_a_feature_not_a_bug.Announcement;

public class AnnouncementTest {

    private Announcement announcement;

    @Before
    public void setUp() {
        announcement = new Announcement("Title", "Description");
    }

    @Test
    public void testGetTitle() {
        assertEquals("Title", announcement.getTitle());
    }

    @Test
    public void testSetTitle() {
        announcement.setTitle("New Title");
        assertEquals("New Title", announcement.getTitle());
    }

    @Test
    public void testGetDescription() {
        assertEquals("Description", announcement.getDescription());
    }

    @Test
    public void testSetDescription() {
        announcement.setDescription("New Description");
        assertEquals("New Description", announcement.getDescription());
    }
}