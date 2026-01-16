package br.edu.ifce.engcomp.francis.diversidade.miscellaneous;

import android.content.Context;
import android.content.Intent;
import android.location.Address;
import android.location.Geocoder;
import android.location.Location;
import android.location.LocationManager;
import android.view.Gravity;
import android.widget.Toast;

import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.model.BitmapDescriptorFactory;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.Marker;
import com.google.android.gms.maps.model.MarkerOptions;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import br.edu.ifce.engcomp.francis.diversidade.R;
import br.edu.ifce.engcomp.francis.diversidade.activities.DetailNucleusActivity;

/**
 * Created by Bolsista on 12/04/2016.
 */
public class NucleusOperations {

    public LatLng getMyPosition(Location location) {
        float myPositionLatitude = (float) location.getLatitude();
        float myPositionLongitude = (float) location.getLongitude();
        LatLng myPosition = new LatLng(myPositionLatitude, myPositionLongitude);

        return myPosition;
    }

    public void initMarkers(GoogleMap map) {
        Marker markerNucleus = map.addMarker(new MarkerOptions().position(new LatLng(-3.7297003, -38.5398319))
                .title("GRAB - Grupo de Resistência Asa Branca")
                .icon(BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_VIOLET)));

        Marker markerEvent = map.addMarker(new MarkerOptions().position(new LatLng(-3.7441914, -38.5380658))
                .title("Lançamento do app Diversidade - 15/05/2016")
                .icon(BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_ROSE)));
    }
}