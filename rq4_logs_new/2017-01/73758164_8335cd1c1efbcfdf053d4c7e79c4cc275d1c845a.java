package jstudio.com.glider;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Build;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.util.Log;
import android.view.MotionEvent;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;

import org.mapsforge.core.model.LatLong;
import org.mapsforge.map.android.graphics.AndroidGraphicFactory;
import org.mapsforge.map.android.util.AndroidUtil;
import org.mapsforge.map.android.util.MapViewerTemplate;
import org.mapsforge.map.layer.overlay.Marker;
import org.mapsforge.map.layer.renderer.TileRendererLayer;
import org.mapsforge.map.rendertheme.InternalRenderTheme;
import org.mapsforge.map.rendertheme.XmlRenderTheme;
import org.mapsforge.map.util.MapViewProjection;

public class NavigationActivity extends MapViewerTemplate {

    @Override
    protected XmlRenderTheme getRenderTheme() {
        return InternalRenderTheme.OSMARENDER;
    }

    @Override
    protected int getLayoutId() {
        return R.layout.activity_navigation;
    }

    @Override
    protected int getMapViewId() {
        return R.id.mapView;
    }

    @Override
    protected String getMapFileName() {
        return "germany.map";
    }

    @Override
    protected void createLayers() {
        TileRendererLayer tileRendererLayer = AndroidUtil.createTileRendererLayer(this.tileCaches.get(0),
                this.mapView.getModel().mapViewPosition, getMapFile(), getRenderTheme(), false, true, false);
        this.mapView.getLayerManager().getLayers().add(tileRendererLayer);
    }

    @Override
    protected void createMapViews() {
        super.createMapViews();

        mapView.setBuiltInZoomControls(false);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
            SharedPreferences sharedPreferences = PreferenceManager.getDefaultSharedPreferences(this);
            boolean hardwareAcceleration = sharedPreferences.getBoolean("", true);
            if (!hardwareAcceleration) {
                mapView.setLayerType(View.LAYER_TYPE_SOFTWARE, null);
            }
        }
    }

    @Override
    protected void createTileCaches() {
        this.tileCaches.add(AndroidUtil.createTileCache(this, getPersistableId(),
                this.mapView.getModel().displayModel.getTileSize(), this.getScreenRatio(),
                this.mapView.getModel().frameBufferModel.getOverdrawFactor()));
    }

    /* OnCreate Zone */
    private EditText endLatitudeEditText;
    private EditText endLongitudeEditText;
    private Intent intent;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        AndroidGraphicFactory.createInstance(this.getApplication());
        super.onCreate(savedInstanceState);
        setTitle(getClass().getSimpleName());

        intent = new Intent(getBaseContext(), MapsActivity.class);

        Button button =(Button) findViewById(R.id.navigationButton);

        button.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                startActivity(intent);
            }
        });

    }

    @Override
    public boolean onTouchEvent(MotionEvent motionEvent) {

        double x = motionEvent.getX();
        double y = motionEvent.getY();

        MapViewProjection projection = new MapViewProjection(mapView);
        LatLong touchedPoint = projection.fromPixels(x, y);

        Marker marker = new Marker(touchedPoint, null, 0, 0);
        mapView.getLayerManager().getLayers().add(marker);

        String endLatitude = Double.toString(touchedPoint.getLatitude()).substring(0,9);
        String endLongitude = Double.toString(touchedPoint.getLongitude()).substring(0,9);

        endLatitudeEditText = (EditText) findViewById(R.id.editEndLatitude);
        endLatitudeEditText.setText(endLatitude);
        endLongitudeEditText = (EditText) findViewById(R.id.editEndLongitude);
        endLongitudeEditText.setText(endLongitude);

        intent.putExtra("EXTRA_IS_NAVIGATION_STARTED", "true");
        intent.putExtra("EXTRA_END_LATITUDE", endLatitude);
        intent.putExtra("EXTRA_END_LONGITUDE", endLongitude);

        Log.i("Recording Coordinates", "text : "+ endLatitude + " " + endLongitude +" : end");

        return true;
    }
}