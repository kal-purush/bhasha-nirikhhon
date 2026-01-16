package com.vbazh.weatherapp_mvp.data.repository.weather;


import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import com.vbazh.weatherapp_mvp.data.entities.Weather;
import com.vbazh.weatherapp_mvp.data.repository.DbContract;
import com.vbazh.weatherapp_mvp.data.repository.DbHelper;
import com.vbazh.weatherapp_mvp.utils.BaseUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WeatherLocalDataSource implements WeatherDataSource {

    private static WeatherLocalDataSource INSTANCE;

    private DbHelper mDbhelper;

    String[] projection = {
            DbContract.WeatherEntry.COLUMN_ID,
            DbContract.WeatherEntry.COLUMN_WEATHER_CITY_ID,
            DbContract.WeatherEntry.COLUMN_WEATHER_CITY_NAME,
            DbContract.WeatherEntry.COLUMN_WEATHER_CITY_LON,
            DbContract.WeatherEntry.COLUMN_WEATHER_CITY_LAT,
            DbContract.WeatherEntry.COLUMN_WEATHER_TEMP,
            DbContract.WeatherEntry.COLUMN_WEATHER_PRESSURE,
            DbContract.WeatherEntry.COLUMN_WEATHER_HUMIDITY
    };

    public WeatherLocalDataSource(Context context) {
        BaseUtils.checkNotNull(context);
        this.mDbhelper = new DbHelper(context);
    }

    public static WeatherLocalDataSource getInstance(Context context) {

        if (INSTANCE == null) {
            INSTANCE = new WeatherLocalDataSource(context);
        }
        return INSTANCE;
    }

    @Override
    public void getWeather(Integer id, GetWeatherCallback callback) {

    }

    @Override
    public void getWeather(String id, GetWeatherCallback callback) {

        SQLiteDatabase db = mDbhelper.getReadableDatabase();

        String selection = DbContract.WeatherEntry.COLUMN_ID + " LIKE ?";
        String[] selectionArgs = {id};

        Cursor c = db.query(DbContract.WeatherEntry.TABLE_NAME, projection, selection, selectionArgs, null, null, null);

        Weather weather = null;

        if (c != null && c.getCount() > 0) {

            c.moveToFirst();
            String idColumn = c.getString(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_ID));
            Integer idCity = c.getInt(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_ID));
            String cityName = c.getString(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_NAME));
            Double cityLong = c.getDouble(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_LON));
            Double cityLat = c.getDouble(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_LAT));
            Double temp = c.getDouble(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_TEMP));
            Integer pressure = c.getInt(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_PRESSURE));
            Integer humidity = c.getInt(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_HUMIDITY));
            weather = new Weather(idColumn, idCity, cityName, cityLong, cityLat, temp, pressure, humidity);
        }
        if (c != null) {
            c.close();
        }
        db.close();

        if (weather != null) {
            callback.onWeatherLoaded(weather);
        } else {
            callback.onDataNotAvailable();
        }


    }

    public void saveWeather(Weather weather, WeatherSavedCallback callback) {

        BaseUtils.checkNotNull(weather);

        SQLiteDatabase db = mDbhelper.getWritableDatabase();

        ContentValues values = new ContentValues();

        values.put(DbContract.WeatherEntry.COLUMN_ID, weather.getId());
        values.put(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_ID, weather.getIdCity());
        values.put(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_NAME, weather.getCityName());
        values.put(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_LON, weather.getCityLon());
        values.put(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_LAT, weather.getCityLat());
        values.put(DbContract.WeatherEntry.COLUMN_WEATHER_TEMP, weather.getTemperature());
        values.put(DbContract.WeatherEntry.COLUMN_WEATHER_PRESSURE, weather.getPressure());
        values.put(DbContract.WeatherEntry.COLUMN_WEATHER_HUMIDITY, weather.getHumidity());

        db.insert(DbContract.WeatherEntry.TABLE_NAME, null, values);
        db.close();
        callback.onWeatherSaved();

    }

    @Override
    public void getAllWeather(GetAllWeatherLoaded callback) {

        List<Weather> allWeather = new ArrayList<>();
        SQLiteDatabase db = mDbhelper.getReadableDatabase();

        Cursor c = db.query(DbContract.WeatherEntry.TABLE_NAME, projection, null, null, null, null, null);

        if (c != null && c.getCount() > 0) {

            while (c.moveToNext()) {

                String idColumn = c.getString(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_ID));
                Integer idCity = c.getInt(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_ID));
                String cityName = c.getString(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_NAME));
                Double cityLong = c.getDouble(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_LON));
                Double cityLat = c.getDouble(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_CITY_LAT));
                Double temp = c.getDouble(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_TEMP));
                Integer pressure = c.getInt(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_PRESSURE));
                Integer humidity = c.getInt(c.getColumnIndexOrThrow(DbContract.WeatherEntry.COLUMN_WEATHER_HUMIDITY));

                Weather weather = new Weather(idColumn, idCity, cityName, cityLong, cityLat, temp, pressure, humidity);
                allWeather.add(weather);
            }
        }
        if (c != null) {
            c.close();
        }

        db.close();

        if (allWeather.isEmpty()) {
            callback.onDataNotAvailable();
        } else {
            callback.onAllWeatherLoaded(allWeather);
        }
    }

    @Override
    public void getAllWeather(Map<String, String> ids, GetAllWeatherLoaded callback) {

    }

    @Override
    public void deleteAllWeather() {

        SQLiteDatabase db = mDbhelper.getWritableDatabase();
        db.delete(DbContract.WeatherEntry.TABLE_NAME, null, null);
        db.close();

    }

    @Override
    public void refreshWeather() {

    }

    @Override
    public void deleteWeather(String id) {

        BaseUtils.checkNotNull(id);

        SQLiteDatabase db = mDbhelper.getWritableDatabase();

        String selection = DbContract.WeatherEntry.COLUMN_ID + " LIKE ?";
        String[] selectionArgs = {String.valueOf(id)};

        db.delete(DbContract.WeatherEntry.TABLE_NAME, selection, selectionArgs);
        db.close();

    }
}