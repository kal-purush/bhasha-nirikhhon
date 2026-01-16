package com.jebysun.android.maptracker;

import java.util.ArrayList;
import java.util.List;

import com.baidu.location.BDLocation;
import com.baidu.location.BDLocationListener;
import com.baidu.location.LocationClient;
import com.baidu.location.LocationClientOption;
import com.baidu.mapapi.SDKInitializer;
import com.baidu.mapapi.map.BaiduMap;
import com.baidu.mapapi.map.BitmapDescriptor;
import com.baidu.mapapi.map.BitmapDescriptorFactory;
import com.baidu.mapapi.map.MapStatus;
import com.baidu.mapapi.map.MapStatusUpdate;
import com.baidu.mapapi.map.MapStatusUpdateFactory;
import com.baidu.mapapi.map.MapView;
import com.baidu.mapapi.map.MarkerOptions;
import com.baidu.mapapi.map.OverlayOptions;
import com.baidu.mapapi.map.PolylineOptions;
import com.baidu.mapapi.model.LatLng;

import android.app.Activity;
import android.os.Bundle;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.ImageButton;

public class MapActivity extends Activity {

	private MapView mMapView;
	private BaiduMap mBaiduMap;
	
	private ImageButton btnZoomIn;
	private ImageButton btnZoomOut;
	
	private List<LatLng> points;
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		
		//在使用SDK各组件之前初始化context信息，传入ApplicationContext  
        //注意该方法要再setContentView方法之前实现  
        SDKInitializer.initialize(getApplicationContext());
        
		setContentView(R.layout.activity_map);
		
		mMapView = (MapView) findViewById(R.id.bmapView);  
		mBaiduMap = mMapView.getMap();
		mMapView.showZoomControls(false);
		mBaiduMap.getUiSettings().setOverlookingGesturesEnabled(false);
		
		btnZoomIn = (ImageButton)this.findViewById(R.id.btn_zoomin);
		btnZoomOut = (ImageButton)this.findViewById(R.id.btn_zoomout);
		btnZoomIn.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				zoomMap("zoomin");
			}
		});
		btnZoomOut.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				zoomMap("zoomout");
			}
		});
		
		init();
		
		drawLine();
		
		//设置地图中心点
		MapStatusUpdate u = MapStatusUpdateFactory.newLatLng(points.get(0));
		mBaiduMap.animateMapStatus(u);
	}
	
	private void init() {
		points = new ArrayList<LatLng>();
		points.add(new LatLng(31.157153, 120.51721));
		points.add(new LatLng(31.156195, 120.516599));
		points.add(new LatLng(31.156072, 120.513904));
		points.add(new LatLng(31.158111, 120.513832));
		points.add(new LatLng(31.160831, 120.513724));
		points.add(new LatLng(31.162345, 120.514048));
		points.add(new LatLng(31.164755, 120.513904));
		points.add(new LatLng(31.167227, 120.513437));
		points.add(new LatLng(31.169081, 120.512502));
		points.add(new LatLng(31.170379, 120.508011));
		points.add(new LatLng(31.173932, 120.509376));
		points.add(new LatLng(31.173438, 120.518719));
		points.add(new LatLng(31.17319, 120.520551));
		points.add(new LatLng(31.17146, 120.520551));
		points.add(new LatLng(31.168432, 120.521342));
	}
	
	/**
	 * 地图缩放
	 * @author JebySun
	 * @date 2015年1月24日 下午6:06:56
	 * @email jebysun@126.com
	 * @param zoom
	 */
	public void zoomMap(String zoom) {
		float zoomLevel = mBaiduMap.getMapStatus().zoom;
		if (zoom.equals("zoomin")) {
			zoomLevel++;
		} else {
			zoomLevel--;
		}
	    MapStatus mMapStatus = new MapStatus.Builder()
	    .zoom(zoomLevel)
	    .build();
	    MapStatusUpdate mMapStatusUpdate = MapStatusUpdateFactory.newMapStatus(mMapStatus);
	    mBaiduMap.animateMapStatus(mMapStatusUpdate);
	}
	
	public void drawLine() {
		//画线..构造对象
		OverlayOptions ooPolyline = new PolylineOptions().color(0xFFFF6600).points(points);
		//添加到地图
		mBaiduMap.addOverlay(ooPolyline);

		BitmapDescriptor bitMapPoint = BitmapDescriptorFactory.fromResource(R.drawable.track_point);  
		BitmapDescriptor bitMapStart = BitmapDescriptorFactory.fromResource(R.drawable.track_start);  
		BitmapDescriptor bitMapEnd = BitmapDescriptorFactory.fromResource(R.drawable.track_end);  
		LatLng p = null;
		OverlayOptions option = null;
		//画点
		for (int i=0; i<points.size(); i++) {
			p = points.get(i);
			if (i==0) {
				option = new MarkerOptions()  
				.position(p)
				.icon(bitMapStart);
			} else if (i==(points.size()-1)) {
				option = new MarkerOptions()  
				.position(p)
				.icon(bitMapEnd);
			} else {
				option = new MarkerOptions()  
				.position(p)
				.anchor(0.5f, 0.5f)
				.icon(bitMapPoint);
			}
			mBaiduMap.addOverlay(option);
		}
	}
	
	@Override
	protected void onPause() {
		mMapView.onPause();
		super.onPause();
	}

	@Override
	protected void onResume() {
		mMapView.onResume();
		super.onResume();
	}

	@Override
	protected void onDestroy() {
		mBaiduMap.setMyLocationEnabled(false);
		mMapView.onDestroy();
		super.onDestroy();
	}
	

}