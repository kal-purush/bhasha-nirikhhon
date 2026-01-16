package com.daclink.citypulse;

import android.content.Context;

import androidx.test.core.app.ApplicationProvider;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import com.daclink.citypulse.database.Activities;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(AndroidJUnit4.class)
public class ActivitiesDaoTest {

    private AppDatabase db;

    @Before
    public void setUp() {
        Context context = ApplicationProvider.getApplicationContext();
        db = AppDatabase.getInstance(context);
        db.activitiesDAO().clearAll();
    }

    @Test
    public void insert() {
        List<Activities> l = db.activitiesDAO().getAll();
        assertEquals(0, l.size());
        l.add(new Activities(
                "1",
                "test",
                "1/1/1",
                "testV",
                "gotham",
                "Sport",
                "nop",
                false
        ));
        db.activitiesDAO().insertAll(l);
        assertEquals(1, db.activitiesDAO().getAll().size());
    }

    @Test
    public void update() {
        List<Activities> l = db.activitiesDAO().getAll();
        l.add(new Activities(
                "1",
                "test",
                "1/1/1",
                "testV",
                "gotham",
                "Sport",
                "nop",
                false
        ));
        db.activitiesDAO().insertAll(l);
        db.activitiesDAO().setWishlistEvent("1", true);
        l = db.activitiesDAO().getAll();
        assertTrue(l.get(0).isWishlisted());
    }

    @Test
    public void delete() {
        db.activitiesDAO().clearAll();
        List<Activities> l = db.activitiesDAO().getAll();
        assertEquals(0, l.size());
    }
}