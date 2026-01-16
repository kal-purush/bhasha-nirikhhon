package finalproject_plants;

import com.baidu.location.BDLocation;
import com.baidu.location.BDLocationListener;
import com.baidu.location.LocationClient;
import com.baidu.location.LocationClientOption;
import com.baidu.location.LocationClientOption.LocationMode;
import com.baidu.mapapi.map.BaiduMap;
import com.baidu.mapapi.map.MapStatusUpdate;
import com.baidu.mapapi.map.MapStatusUpdateFactory;
import com.baidu.mapapi.map.MapView;
import com.baidu.mapapi.model.LatLng;

import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

/**
 * Created by zhang on 2017/10/22.
 */

public class MapFragment extends Fragment {

    // 百度地图相关
    private MapView mMapView = null;
    private BaiduMap mBaiduMap = null;
    private MapStatusUpdate msu = null;

    // 定位相关
    private boolean isFirstGetLocation=true;
    private LocationClient mLocationClient = null;
    MyLocationListener mLocationListener;


    @Override
    public void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);
    }

    /**
     * 初始化地图控件
     */
    private void InitiMapView(View view)
    {
        mMapView = (MapView) view.findViewById(R.id.map_view);
        mBaiduMap = mMapView.getMap();// 从地图视图中获取百度地图实例对象
// 设置地图初始化缩放比例
        msu = MapStatusUpdateFactory.zoomTo(15.0f);// 这里的显示等级第15级
        mBaiduMap.setMapStatus(msu);
    }

    /**
     * 初始化定位相关
     */
    private void InitiMapLocation()
    {
        mLocationClient=new LocationClient(getActivity().getApplicationContext()); //定位客户端
        mLocationListener=new MyLocationListener();  //定位监听器
//绑定定位监听器
        mLocationClient.registerLocationListener(mLocationListener);

//对定位参数进行配置 LocationClientOption
        LocationClientOption option=new LocationClientOption();
        option.setLocationMode(LocationMode.Hight_Accuracy);
        option.setCoorType("bd09ll");
        option.setScanSpan(1000);
        option.setIsNeedAddress(true);
        option.setOpenGps(true);

//设置定位参数
        mLocationClient.setLocOption(option);
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState)
    {
        View view = inflater.inflate(R.layout.map_fragment, container, false);
        InitiMapView(view); //初始化地图相关内容
        InitiMapLocation(); //初始化地图定位相关内容
        mBaiduMap.setMyLocationEnabled(true);//开启定位图层
        return view;
    }


    @Override
    public void onStart()
    {
        super.onStart();
        if (mLocationClient.isStarted()==false)
        {
            mLocationClient.start();//开启定位
        }
    }

    @Override
    public void onResume()
    {
        super.onResume();
        if (mMapView != null)
        {
            mMapView.onResume(); // 使百度地图地图控件和Fragment的生命周期保持一致
        }

    }

    @Override
    public void onPause()
    {
        super.onPause();
        if (mMapView != null)
        {
            mMapView.onPause(); // 使百度地图地图控件和Fragment的生命周期保持一致
        }
    }

    @Override
    public void onStop()
    {
        super.onStop();
        mBaiduMap.setMyLocationEnabled(false);
        mLocationClient.stop();//停止定位
    }

    @Override
    public void onDestroy()
    {
        super.onDestroy();
        if (mMapView != null)
        {
            mMapView.onDestroy(); // 使百度地图地图控件和Fragment的生命周期保持一致
        }
    }

    public class MyLocationListener implements BDLocationListener
    {
        @Override
        public void onReceiveLocation(BDLocation location)
        {

//判断是否为首次获取到位置数据
            if (isFirstGetLocation)
            {
//如果为首次定位，则直接定位到当前用户坐标
                LatLng latLng=new LatLng(location.getLatitude(), location.getLongitude());
                MapStatusUpdate msuLocationMapStatusUpdate=MapStatusUpdateFactory//
                        .newLatLng(latLng);
                mBaiduMap.animateMapStatus(msuLocationMapStatusUpdate);

                isFirstGetLocation=false;
            }
        }
    }


}

