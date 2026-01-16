package com.dam.asfaltame.Maps;

import android.content.Context;

import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.model.BitmapDescriptor;
import com.google.android.gms.maps.model.BitmapDescriptorFactory;
import com.google.android.gms.maps.model.MarkerOptions;
import com.google.maps.android.clustering.ClusterManager;
import com.google.maps.android.clustering.view.DefaultClusterRenderer;

public class MapClusterRenderer extends DefaultClusterRenderer<MapItem>{

    private final Context mContext;

    public MapClusterRenderer(Context context, GoogleMap map, ClusterManager<MapItem> clusterManager) {
        super(context, map, clusterManager);
        mContext = context;
    }

    @Override protected void onBeforeClusterItemRendered(MapItem item, MarkerOptions markerOptions) {
        final BitmapDescriptor markerDescriptor = BitmapDescriptorFactory.fromResource(item.getmIconId());
        markerOptions.icon(markerDescriptor);
    }
}