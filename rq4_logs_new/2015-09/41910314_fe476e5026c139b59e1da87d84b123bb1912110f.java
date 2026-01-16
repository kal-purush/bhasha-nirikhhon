package ie.nasouth.android.naireland;

import com.google.android.gms.maps.model.LatLng;
import com.google.maps.android.clustering.ClusterItem;

import java.util.ArrayList;
import java.util.List;

/**
 * An instance of this class stores everything about an individual meeting that we need to
 * draw a marker withan infowindow on the map
 */
public class MyMeetingLocation implements ClusterItem {
    private final LatLng mPosition;
    public double lat;
    public double lng;
    public String meetingName;
    public String meetingAddress;

    // An array of ArrayLists to store the list of daily meetings times
    public ArrayList<String>[]  meetingTimes = new ArrayList[7];


    public MyMeetingLocation(double lat, double lng) {

        mPosition = new LatLng(lat, lng);
        this.lat = lat;
        this.lng = lng;
    }

    @Override
    public LatLng getPosition() {
        return mPosition;
    }

    public double getLong() {
        return this.lng;
    }

    public double getLat() {
        return this.lat;
    }

    public String getMeetingName() {
        return meetingName;
    }

    public void setMeetingName(String meetingName) {
        this.meetingName = meetingName;
    }

    public String getMeetingAddress() {
        return meetingAddress;
    }

    public void setMeetingAddress(String meetingAddress) {
        this.meetingAddress = meetingAddress;
    }

    public void addMeetingTime(int dayOfWeek, String meetingTime) {
        this.meetingTimes[dayOfWeek].add(meetingTime);
    }

}