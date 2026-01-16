package com.daclink.citypulse;

import android.content.Context;

import androidx.room.Room;
import androidx.test.core.app.ApplicationProvider;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import com.daclink.citypulse.model.CachedEvent;
import com.daclink.citypulse.model.CachedEventDao;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(AndroidJUnit4.class)
public class CachedEventDaoTest {

    private AppDatabase db;
    private CachedEventDao dao;

    @Before
    public void setup() {
        Context context = ApplicationProvider.getApplicationContext();
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase.class)
                .allowMainThreadQueries()
                .build();
        dao = db.cachedEventDao();
    }

    @After
    public void teardown() {
        db.close();
    }

    @Test
    public void insertCachedEventTest() {
        CachedEvent event = new CachedEvent("api_101", "Monterey Jazz", "2025-06-01", "City Hall", "Monterey", "Music", "https://img.com/jazz.jpg", false);
        dao.insertAll(Collections.singletonList(event));
        List<CachedEvent> results = dao.getEventsByCityAndCategory("Monterey", "Music");
        assertEquals(1, results.size());
        assertEquals("Monterey Jazz", results.get(0).getTitle());
    }

    @Test
    public void updateWishlistedTest() {
        CachedEvent event = new CachedEvent("api_102", "Carmel Art Fest", "2025-07-15", "Ocean Ave", "Carmel", "Art", "https://img.com/art.jpg", false);
        dao.insertAll(Collections.singletonList(event));
        dao.setWishlistEvent("api_102", true);
        List<CachedEvent> results = dao.getEventsByCityAndCategory("Carmel", "Art");
        assertTrue(results.get(0).isWishlisted());
    }

    @Test
    public void deleteByCityAndCategoryTest() {
        CachedEvent event = new CachedEvent("api_103", "Taste of Marina", "2025-08-10", "Harbor Walk", "Marina", "Food", "https://img.com/food.jpg", false);
        dao.insertAll(Collections.singletonList(event));
        dao.deleteEventsForCityAndCategory("Marina", "Food");
        List<CachedEvent> results = dao.getEventsByCityAndCategory("Marina", "Food");
        assertTrue(results.isEmpty());
    }
}