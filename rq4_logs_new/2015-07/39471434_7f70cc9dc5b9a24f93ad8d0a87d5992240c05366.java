package org.swiftdeal.weather.app.test;

import android.database.sqlite.SQLiteDatabase;
import android.test.AndroidTestCase;

import org.swiftdeal.weather.data.WeatherDbHelper;

/**
 * Created by Faizan Ayubi on 27-07-2015.
 */
public class TestDb extends AndroidTestCase {
    public void testCreateDb() throws Throwable {
        mContext.deleteDatabase(WeatherDbHelper.DATABASE_NAME);
        SQLiteDatabase db = new WeatherDbHelper(this.mContext).getWritableDatabase();
        assertEquals(true, db.isOpen());
        db.close();
    }
}