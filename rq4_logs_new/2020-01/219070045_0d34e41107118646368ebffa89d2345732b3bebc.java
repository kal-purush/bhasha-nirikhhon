package com.example.eventmanageri.Activities;

import androidx.appcompat.app.AppCompatActivity;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.util.Log;
import android.widget.Toast;

import com.example.eventmanageri.R;

import org.osmdroid.api.IMapController;
import org.osmdroid.config.Configuration;
import org.osmdroid.events.MapEventsReceiver;
import org.osmdroid.tileprovider.tilesource.TileSourceFactory;
import org.osmdroid.util.GeoPoint;
import org.osmdroid.views.MapView;


import java.util.ArrayList;

import org.osmdroid.views.overlay.ItemizedIconOverlay;
import org.osmdroid.views.overlay.ItemizedOverlayWithFocus;
import org.osmdroid.views.overlay.MapEventsOverlay;
import org.osmdroid.views.overlay.OverlayItem;

public class LocationActivity extends AppCompatActivity {
    private static final String TAG = "LocationActivity";

    private MapView map = null;

    // Data reference
    private String key, userid, email, title, date, type, memo, photo, video, location, share;

    // Activity reference
    public interface ActivityConstants {
        int ACTIVITY_1 = 1001;  // NewEventActivity
        int ACTIVITY_2 = 1002;  // EventModActivity
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_location);

        // Get data from the calling activity
        key = getIntent().getStringExtra("key");
        userid = getIntent().getStringExtra("userid");
        email = getIntent().getStringExtra("email");
        title = getIntent().getStringExtra("title");
        memo = getIntent().getStringExtra("memo");
        photo = getIntent().getStringExtra("photo");
        video = getIntent().getStringExtra("video");
        date = getIntent().getStringExtra("date");
        type = getIntent().getStringExtra("type");
        share = getIntent().getStringExtra("share");
        Log.d("LocationActivity", "sss View key value (map - location msa):");

        // Check the calling activity and return data
        int callingActivity = getIntent().getIntExtra("calling-activity", 0);
        switch (callingActivity) {
            case ActivityConstants.ACTIVITY_1:
                //goToNewEvent();
                break;
            case ActivityConstants.ACTIVITY_2:
                goToEventMod();
                break;
        }

        // Define UI reference

        Context ctx = getApplicationContext();
        Configuration.getInstance().load(ctx, PreferenceManager.getDefaultSharedPreferences(ctx));

        // TODO: inflate and create map
        setContentView(R.layout.activity_location);
        map = (MapView) findViewById(R.id.map);
        map.setTileSource(TileSourceFactory.MAPNIK);

        // TODO: add default zoom buttons
        map.setMultiTouchControls(true);

        // TODO: coordinate (Luxembourg)
        GeoPoint startPoint = new GeoPoint(49.503759, 5.948007);

        // TODO: set the default zoom and position of the map
        IMapController mapController = map.getController();
        mapController.setCenter(startPoint);
        mapController.setZoom(13.0);

        // TODO: add a marker at the position of Maison du Nombre
      /*  ArrayList<OverlayItem> items = new ArrayList<OverlayItem>();
        items.add(new OverlayItem("Location", "Event", new GeoPoint(49.503759,5.948007)));

        // TODO: setup an event listener to add and remove overlay items
        ItemizedOverlayWithFocus<OverlayItem> overlay = new ItemizedOverlayWithFocus<OverlayItem>(items,
                new ItemizedIconOverlay.OnItemGestureListener<OverlayItem>() {
                    @Override
                    public boolean onItemSingleTapUp(final int index, final OverlayItem item) {
                        return true;
                    }

                    @Override
                    public boolean onItemLongPress(final int index, final OverlayItem item) {
                        return false;
                    }
                }, ctx);

        // TODO add an overlay to the map
        overlay.setFocusItemsOnTap(true);
        map.getOverlays().add(overlay);*/

        final MapEventsReceiver mReceive = new MapEventsReceiver() {


            @Override
            public boolean singleTapConfirmedHelper(GeoPoint p) {
                Toast.makeText(getBaseContext(),"event location" + " - " + p.getLatitude() + " - "+p.getLongitude(),Toast.LENGTH_LONG).show();
                return false;
            }

            @Override
            public boolean longPressHelper(GeoPoint p) {
                return false;
            }

        };
        map.getOverlays().add(new MapEventsOverlay(mReceive));

    }

    @Override
    public void onResume(){
        super.onResume();
        map.onResume();
    }

    @Override
    public void onPause(){
        super.onPause();
        map.onPause();
    }


    // Return data to NewEventActivity
    private void goToNewEvent() {
        Intent intent = new Intent(LocationActivity.this, NewEventActivity.class);
        intent.putExtra("date", date);
        intent.putExtra("key", key);
        intent.putExtra("userid", userid);
        intent.putExtra("email", email);
        intent.putExtra("title", title);
        intent.putExtra("memo", memo);
        intent.putExtra("photo", photo);
        intent.putExtra("video", video);
        intent.putExtra("location", location);
        intent.putExtra("type", type);
        intent.putExtra("share", share);
        startActivity(intent);
    }

    // Return data to EventModActivity
    private void goToEventMod() {
        Intent intent = new Intent(LocationActivity.this, EventModActivity.class);
        intent.putExtra("date", date);
        intent.putExtra("key", key);
        intent.putExtra("userid", userid);
        intent.putExtra("email", email);
        intent.putExtra("title", title);
        intent.putExtra("memo", memo);
        intent.putExtra("photo", photo);
        intent.putExtra("video", video);
        intent.putExtra("location", location);
        intent.putExtra("type", type);
        intent.putExtra("share", share);
        startActivity(intent);
    }
}