package com.example.tomek.etapi;

import android.Manifest;
import android.content.Context;
import android.content.pm.PackageManager;
import android.location.Criteria;
import android.location.Location;
import android.location.LocationManager;
import android.support.v4.app.ActivityCompat;

/**
 * Class containing functions used to reading current location
 * Created by Tomek on 23.12.2016.
 * @author TomaszBorowicz
 * @
 */

public class ReadLocation {
    private Criteria crit;
    private LocationManager locMan;
    private Location loc;

    /**
     * Method returns current location in String format
     * Location is read from GPS
     * @param mContext Context of the activity which calls the method.
     * @return String in format Longitude Latitude
     */
    public String readLocationByGPS(Context mContext) {
        return new String(readLongitudeByNetwork(mContext) + "    " + readLatitudeByNetwork(mContext));
    }

    /**
     * Method returns current location in String format
     * Location is read from Network
     * @param mContext Context of the activity which calls the method.
     * @return String in format Longitude Latitude
     */
    public String readLocationByNetwork(Context mContext) {
        return new String(readLongitudeByGPS(mContext) + "    " + readLatitudeByGPS(mContext));
    }

    /**
     * Method returns current location in String format
     * Location is read from the best source (Network or GPS)
     * @param mContext Context of the activity which calls the method.
     * @return String in format Longitude Latitude
     */
    public String readLocationByBest(Context mContext) {
        return new String(readLongitudeByBest(mContext) + "    " + readLatitudeByBest(mContext));
    }

    /**
     * Method returns current Longitude read from GPS
     * @param mContext Context of the activity which calls the method.
     * @return Longitude in double format
     */
    public double readLongitudeByGPS(Context mContext) {
        crit = new Criteria();
        locMan = (LocationManager) mContext.getSystemService(mContext.LOCATION_SERVICE);
        loc = locMan.getLastKnownLocation(locMan.GPS_PROVIDER);
        return loc.getLongitude();
    }

    /**
     * Method returns current Latitude read from GPS
     * @param mContext Context of the activity which calls the method.
     * @return Latitude in double format
     */
    public double readLatitudeByGPS(Context mContext) {
        crit = new Criteria();
        locMan = (LocationManager) mContext.getSystemService(mContext.LOCATION_SERVICE);
        loc = locMan.getLastKnownLocation(locMan.GPS_PROVIDER);
        return loc.getLatitude();
    }

    /**
     * Method returns current Longitude read from Network
     * @param mContext Context of the activity which calls the method.
     * @return Longitude in double format
     */
    public double readLongitudeByNetwork(Context mContext) {
        crit = new Criteria();
        locMan = (LocationManager) mContext.getSystemService(mContext.LOCATION_SERVICE);
        loc = locMan.getLastKnownLocation(locMan.NETWORK_PROVIDER);
        return loc.getLongitude();
    }

    /**
     * Method returns current Latitude read from Network
     * @param mContext Context of the activity which calls the method.
     * @return Latitude in double format
     */
    public double readLatitudeByNetwork(Context mContext) {
        crit = new Criteria();
        locMan = (LocationManager) mContext.getSystemService(mContext.LOCATION_SERVICE);
        loc = locMan.getLastKnownLocation(locMan.NETWORK_PROVIDER);
        return loc.getLatitude();
    }

    /**
     * Method returns current Longitude read from the best source (Network ot GPS)
     * @param mContext Context of the activity which calls the method.
     * @return Longitude in double format
     */
    public double readLongitudeByBest(Context mContext) {
        crit = new Criteria();
        locMan = (LocationManager) mContext.getSystemService(mContext.LOCATION_SERVICE);
        if (ActivityCompat.checkSelfPermission(mContext, Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED && ActivityCompat.checkSelfPermission(mContext, Manifest.permission.ACCESS_COARSE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            // TODO: Consider calling
            //    ActivityCompat#requestPermissions
            // here to request the missing permissions, and then overriding
            //   public void onRequestPermissionsResult(int requestCode, String[] permissions,
            //                                          int[] grantResults)
            // to handle the case where the user grants the permission. See the documentation
            // for ActivityCompat#requestPermissions for more details.
            return 0.00;
        }
        loc = locMan.getLastKnownLocation(locMan.getBestProvider(crit, true));
        return loc.getLongitude();
    }

    /**
     * Method returns current Latitude read from the best source (Network ot GPS)
     * @param mContext Context of the activity which calls the method.
     * @return Latitude in double format
     */
    public double readLatitudeByBest(Context mContext) {
        crit = new Criteria();
        locMan = (LocationManager) mContext.getSystemService(mContext.LOCATION_SERVICE);
        if (ActivityCompat.checkSelfPermission(mContext, Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED && ActivityCompat.checkSelfPermission(mContext, Manifest.permission.ACCESS_COARSE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            // TODO: Consider calling
            //    ActivityCompat#requestPermissions
            // here to request the missing permissions, and then overriding
            //   public void onRequestPermissionsResult(int requestCode, String[] permissions,
            //                                          int[] grantResults)
            // to handle the case where the user grants the permission. See the documentation
            // for ActivityCompat#requestPermissions for more details.
            return 0.00;
        }
        loc = locMan.getLastKnownLocation(locMan.getBestProvider(crit, true));
        return loc.getLatitude();
    }

}