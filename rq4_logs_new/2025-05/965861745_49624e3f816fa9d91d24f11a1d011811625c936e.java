package com.daclink.citypulse;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import com.daclink.citypulse.model.EventItem;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;


@RunWith(AndroidJUnit4.class)
public class ActivitiesTest {
    private EventItem e;

    @Before
    public void setUp() {
        e = new EventItem(
                "66",
                "Vador",
                new EventItem.Dates("05/04/2025"),
                null,
                null,
                false);
    }
    @Test
    public void nullImage() {
        String url = e.getImageUrl();
        assertNull(url);
        e.setImageUrl("notUrl");
        assertEquals("notUrl", e.getImageUrl());
    }

    @Test
    public void nullVenue() {
        String venue = e.getVenueName();
        assertEquals("Unknown venue", venue);
        e.setVenueCity("DeathStar");
        assertEquals("DeathStar", e.getVenueName());
    }
}