package com.daclink.citypulse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import android.content.Context;
import android.content.SharedPreferences;
import androidx.test.core.app.ApplicationProvider;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.filters.MediumTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
@MediumTest // For Activity lifecycle
public class LoginTest {

    private UserDao userDao;
    private AppDatabase db;
    private SharedPreferences sharedPreferences;
    private static final String TEST_USERNAME = "testuser1";
    private static final String TEST_PASSWORD = "testuser1";
    private static final String PREFS_NAME = "MyAppPrefs";

    @Rule
    public ActivityScenarioRule<LoginActivity> activityRule =
            new ActivityScenarioRule<>(LoginActivity.class);

    @Before
    public void setUp() {
        Context context = ApplicationProvider.getApplicationContext();

        db = AppDatabase.getInstance(context);
        userDao = db.userDao();

        // Clear existing users and SharedPreferences for a clean test environment
        AppDatabase.databaseWriteExecutor.execute(() -> {
            userDao.getAllUsers().forEach(user -> userDao.delete(user));
        });

        sharedPreferences = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.clear();
        editor.apply();

        // Brief pause to allow DB and SharedPreferences cleanup to complete
        try { Thread.sleep(100); } catch (InterruptedException e) { e.printStackTrace(); }
    }

    @After
    public void tearDown() {
        // Cleanup database and SharedPreferences
        AppDatabase.databaseWriteExecutor.execute(() -> {
            userDao.getAllUsers().forEach(user -> userDao.delete(user));
        });
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.clear();
        editor.apply();
        try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
    }

    @Test
    public void testLoginSuccess() { // updates Prefs and Finishes Activity
        // Insert a test user
        User testUser = new User(TEST_USERNAME, TEST_PASSWORD, false);
        AppDatabase.databaseWriteExecutor.execute(() -> userDao.insert(testUser));
        try { Thread.sleep(200); } catch (InterruptedException e) { e.printStackTrace(); } // Wait for DB insert

        // call logic on the LoginActivity
        activityRule.getScenario().onActivity(activity -> {
            activity.attemptLogin(TEST_USERNAME, TEST_PASSWORD);
        });

        try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }

        // Assert Check SharedPreferences and activity state
        assertTrue("SharedPreferences should indicate logged_in is true", sharedPreferences.getBoolean("logged_in", false));
        assertEquals("SharedPreferences should store correct username", TEST_USERNAME, sharedPreferences.getString("username", null));
        assertFalse("SharedPreferences should store isAdmin as false", sharedPreferences.getBoolean("isAdmin", true)); // Assuming test user is not admin

        activityRule.getScenario().onActivity(activity -> {
            assertTrue("LoginActivity should be finishing after successful login", activity.isFinishing());
        });
    }

    @Test
    public void testLoginFailure() { // Invalid Credentials does not update Prefs and stays on Activity
        // call login logic with incorrect credentials
        activityRule.getScenario().onActivity(activity -> {
            activity.attemptLogin("wronguser", "wrongpassword");
        });

        // Wait for SharedPreferences updates
        try { Thread.sleep(200); } catch (InterruptedException e) { e.printStackTrace(); }

        // Check SharedPreferences and activity state is not updated
        assertFalse("SharedPreferences should indicate logged_in is false after failed login", sharedPreferences.getBoolean("logged_in", false));

        activityRule.getScenario().onActivity(activity -> {
            assertFalse("LoginActivity should NOT be finishing after failed login", activity.isFinishing());
        });
    }
}