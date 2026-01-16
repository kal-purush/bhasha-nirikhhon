package com.daclink.citypulse;

import android.content.Context;

import androidx.room.Room;
import androidx.test.core.app.ApplicationProvider;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import com.daclink.citypulse.AppDatabase;
import com.daclink.citypulse.User;
import com.daclink.citypulse.UserDao;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(AndroidJUnit4.class)
public class UserDaoTest {

    private AppDatabase db;
    private UserDao userDao;

    @Before
    public void setUp() {
        Context context = ApplicationProvider.getApplicationContext();
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase.class)
                .allowMainThreadQueries()
                .build();
        userDao = db.userDao();
    }

    @After
    public void tearDown() throws IOException {
        db.close();
    }

    @Test
    public void testInsertUser() {
        User user = new User("sarah_doe", "hunter2", false);
        userDao.insert(user);

        User found = userDao.getUserByCredentials("sarah_doe", "hunter2");
        assertNotNull(found);
        assertEquals("sarah_doe", found.getUsername());
    }

    @Test
    public void testUpdateUser() {
        User user = new User("james", "abc123", false);
        userDao.insert(user);

        user.setPassword("newPass123");
        userDao.update(user);

        User updated = userDao.getUserByCredentials("james", "newPass123");
        assertNotNull(updated);
    }

    @Test
    public void testDeleteUser() {
        User user = new User("maria", "secure!", true);
        userDao.insert(user);
        userDao.delete(user);

        User result = userDao.getUserByCredentials("maria", "secure!");
        assertNull(result);
    }
}