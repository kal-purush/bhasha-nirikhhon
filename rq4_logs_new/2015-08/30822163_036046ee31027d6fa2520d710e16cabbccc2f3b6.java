package motocitizen.accident;

import android.content.Context;
import android.location.Location;
import android.location.LocationManager;
import android.os.Bundle;

import com.google.android.gms.maps.model.LatLng;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import motocitizen.app.general.user.Role;
import motocitizen.content.Medicine;
import motocitizen.content.AccidentStatus;
import motocitizen.content.Type;
import motocitizen.geolocation.MyLocationManager;
import motocitizen.startup.Preferences;

import static motocitizen.content.AccidentStatus.ACTIVE;
import static motocitizen.content.AccidentStatus.ENDED;
import static motocitizen.content.AccidentStatus.HIDDEN;

public class Accident {
    private static final String[] prerequisites = {"id", "owner_id", "owner", "status", "uxtime", "address", "descr", "lat", "lon", "mc_accident_orig_type", "mc_accident_orig_med", "messages", "history", "onway"};
    Context context;
    Integer rowId;
    private int                     id;
    private int                     ownerId;
    private String                  owner;
    private AccidentStatus          status;
    private Date                    time;
    private String                  address;
    private String                  description;
    private double                  lat;
    private double                  lon;
    private Type                    type;
    private Medicine                medicine;
    private boolean                 self;
    private boolean                 noError;
    private Map<Integer, Message>   messages;
    private Map<Integer, Volunteer> volunteers;
    private Map<Integer, History>   history;

    public Accident(Context context, JSONObject json) {
        update(json);
        this.context = context;
    }

    public void update(JSONObject json) {
        noError = checkPrerequisites(json);
        if (noError) try {
            id = json.getInt("id");
            ownerId = json.getInt("owner_id");
            owner = json.getString("owner");
            status = AccidentStatus.parse(json.getString("status"));
            type = Type.parseType(json.getString("mc_accident_orig_type"));
            medicine = Medicine.parse(json.getString("mc_accident_orig_med"));
            time = new Date(json.getLong("uxtime") * 1000);
            address = json.getString("address");
            description = json.getString("descr");
            lat = json.getDouble("lat");
            lon = json.getDouble("lon");
            self = ownerId == Preferences.getUserId();
            parseMessages(json.getJSONArray("messages"));
            parseVolunteers(json.getJSONArray("onway"));
            parseHistory(json.getJSONArray("history"));
        } catch (Exception e) {
            e.printStackTrace();
            noError = false;
        }
    }

    private static boolean checkPrerequisites(JSONObject message) {
        for (String field : prerequisites) {
            if (!message.has(field)) return false;
        }
        return true;
    }

    private void parseMessages(JSONArray json) throws JSONException {
        if (messages == null) messages = new HashMap<>();
        for (int i = 0; i < json.length(); i++) {
            Message message = new Message(json.getJSONObject(i));
            if (messages.containsKey(message.getId())) continue;
            if (message.isNoError()) messages.put(message.getId(), message);
        }
    }

    private void parseVolunteers(JSONArray json) throws JSONException {
        if (volunteers == null) volunteers = new HashMap<>();
        for (int i = 0; i < json.length(); i++) {
            Volunteer volunteer = new Volunteer(json.getJSONObject(i));
            if (volunteer.isNoError()) volunteers.put(volunteer.getId(), volunteer);
        }
    }

    private void parseHistory(JSONArray json) throws JSONException {
        if (history == null) history = new HashMap<>();
        for (int i = 0; i < json.length(); i++) {
            History action = new History(json.getJSONObject(i));
            if (action.isNoError()) history.put(action.getId(), action);
        }
    }

    public Accident(Context context, Bundle extras) {
        JSONObject temp = new JSONObject();
        try {
            temp.put("id", extras.getString("id"));
            temp.put("lat", extras.getString("lat"));
            temp.put("lon", extras.getString("lon"));
            temp.put("owner_id", extras.getString("owner_id"));
            temp.put("owner", extras.getString("owner"));
            temp.put("status", extras.getString("status"));
            temp.put("uxtime", String.valueOf(System.currentTimeMillis() / 1000L)); //TODO Fuckup
            temp.put("address", extras.getString("address"));
            temp.put("descr", extras.getString("descr"));
            temp.put("mc_accident_orig_type", extras.getString("mc_accident_orig_type"));
            temp.put("mc_accident_orig_med", extras.getString("mc_accident_orig_med"));
            temp.put("messages", new JSONArray("[]"));
            temp.put("history", new JSONArray("[]"));
            temp.put("onway", new JSONArray("[]"));
            update(temp);
        } catch (JSONException e) {
            e.printStackTrace();
            noError = false;
        }
    }

    public boolean isOwner() {
        return isSelf();
    }

    public boolean isSelf() {
        return self;
    }

    public String getTypeString() {
        return Type.getTypeString(getType());
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getMedicineString() {
        return Medicine.getMedicineString(getMedicine());
    }

    public Medicine getMedicine() {
        return medicine;
    }

    public void setMedicine(Medicine medicine) {
        this.medicine = medicine;
    }

    public String getStatusString() {
        return AccidentStatus.getStatusString(getStatus());
    }

    public AccidentStatus getStatus() {
        return status;
    }

    public void setStatus(AccidentStatus status) {
        this.status = status;
    }

    public String getDistanceString() {
        Double distance = getDistanceFromUser();
        if (distance == null) {
            return "? км";
        } else {
            if (distance > 1000) {
                return String.valueOf(Math.round(distance / 10) / 100) + "км";
            } else {
                return String.valueOf(Math.round(distance)) + "м";
            }
        }
    }

    public Double getDistanceFromUser() {
        Location userLocation = MyLocationManager.getLocation(context);
        return (double) getLocation().distanceTo(userLocation);
    }

    public Location getLocation() {
        Location location = new Location(LocationManager.GPS_PROVIDER);
        location.setLatitude(lat);
        location.setLongitude(lon);
        return location;
    }

    public int getUnreadMessagesCount() {
        int counter = 0;
        for (int key : messages.keySet()) {
            if (messages.get(key).isUnread()) counter++;
        }
        return counter;
    }

    public boolean isInvisible() {
        return (status != HIDDEN || Role.isModerator()) && getDistanceFromUser() < Preferences.getVisibleDistance() * 1000 && Preferences.isHidden(getType());
    }

    public Volunteer getVolunteer(int id) {
        return getVolunteers().get(id);
    }

    public Map<Integer, Volunteer> getVolunteers() {
        return volunteers;
    }

    public int getId() {
        return id;
    }

    public int getOwnerId() {
        return ownerId;
    }

    public String getOwner() {
        return owner;
    }

    public Date getTime() {
        return time;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description.replace("^\\s+", "").replace("\\s+$", "");
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public boolean isNoError() {
        return noError;
    }

    public Integer getRowId() {
        return rowId;
    }

    public void setRowId(Integer rowId) {
        this.rowId = rowId;
    }

    public Map<Integer, Message> getMessages() {
        return messages;
    }

    public Map<Integer, History> getHistory() {
        return history;
    }

    public boolean isActive() {
        return status == ACTIVE;
    }

    public boolean isEnded() {
        return status == ENDED;
    }

    public boolean isHidden() {
        return status == HIDDEN;
    }

    public void setLatLng(LatLng latLng) {
        lat = latLng.latitude;
        lon = latLng.longitude;
    }

    public boolean isAccident() {
        return getType() == Type.MOTO_AUTO || getType() == Type.MOTO_MOTO || getType() == Type.MOTO_MAN;
    }
}