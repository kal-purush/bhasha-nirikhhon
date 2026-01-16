package com.weathermap.android;

import android.app.ProgressDialog;
import android.app.Service;
import android.content.Intent;
import android.os.Binder;
import android.os.IBinder;
import android.util.Log;
import android.view.View;
import android.widget.Toast;

import com.weathermap.android.db.City;
import com.weathermap.android.db.County;
import com.weathermap.android.db.Province;
import com.weathermap.android.util.HttpUtil;
import com.weathermap.android.util.Utility;

import org.litepal.crud.DataSupport;

import java.io.IOException;
import java.util.List;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Response;

import static android.content.ContentValues.TAG;

public class MyService extends Service {
    public static final int LEVEL_PROVINCE=0;
    public static final int LEVEL_CITY=1;
    public static final int LEVEL_COUNTY=2;
//    private ProgressDialog progressDialog;
    private List<Province> provinceList;
    private List<City> cityList;
    private List<County> countyList;
    private Province selectedProvince;
    private City selectedCity;
    private County selectedCounty;
    private int currentLevel;


    public MyService() {
    }

    private HelpBinder mBinder=new HelpBinder();

    class HelpBinder extends Binder{
        public void start(){

        }

        public void stop(){

        }
    }

    @Override
    public IBinder onBind(Intent intent) {
        // TODO: Return the communication channel to the service.
    //    throw new UnsupportedOperationException("Not yet implemented");
        return mBinder;
    }

    @Override
    public void onCreate(){
        super.onCreate();
    }

    @Override
    public int onStartCommand(Intent intent,int flags,int startId){
        String province=intent.getStringExtra("province");
        String city=intent.getStringExtra("city");
        String county=intent.getStringExtra("county");
        queryProvinces(province);
        queryCities(city);
        queryCounties(county);
 //       if (currentLevel==LEVEL_COUNTY)
        String weatherId=selectedCounty.getWeatherId();
        Intent intent1=new Intent(this,WeatherActivity.class);
        intent1.putExtra("weather_id",weatherId);
        startActivity(intent1);
        Log.d(TAG, "onStartCommand:hhhhhhhhhhhhhhhhhh " + weatherId);
        return super.onStartCommand(intent,flags,startId);
    }

    @Override
    public void onDestroy(){
        super.onDestroy();
    }


    private void queryProvinces(String goalProvince){
        provinceList= DataSupport.findAll(Province.class);
        if(provinceList.size()>0){
            for(Province province:provinceList){
                if((province.getProvinceName()).equals(goalProvince.substring(0, goalProvince.length() - 1)))
                {
                    selectedProvince=province;
                }
            }
            currentLevel=LEVEL_PROVINCE;
        }else{
            String address="http://guolin.tech/api/china";
            queryFromServer(address,"province",goalProvince);
        }
    }

    private void queryCities(String goalCity){
        cityList= DataSupport.where("provinceid = ?",String.valueOf(selectedProvince.getId())).find(City.class);
        if(cityList.size()>0){
            for(City city:cityList){
                if((city.getCityName()).equals(goalCity.substring(0,goalCity.length()-1))){
                    selectedCity=city;
                }
            }
            currentLevel=LEVEL_CITY;
        }else{
            int provinceCode=selectedProvince.getProvinceCode();
            String address="http://guolin.tech/api/china/"+provinceCode;
            queryFromServer(address,"city",goalCity);
        }
    }

    private  void  queryCounties(String goalCounty){
        countyList=DataSupport.where("cityid = ?", String.valueOf(selectedCity.getId())).find(County.class);
        if(countyList.size()>0){
            for(County county:countyList){
                if((county.getCountyName()).equals(goalCounty.substring(0,goalCounty.length()-1))){
                    selectedCounty=county;
                }
            }
            currentLevel=LEVEL_COUNTY;
        }else{
            int provinceCode=selectedProvince.getProvinceCode();
            int cityCode=selectedCity.getCityCode();
            String address="http://guolin.tech/api/china/"+provinceCode+"/"+cityCode;
            queryFromServer(address,"county",goalCounty);
        }
    }

    private void queryFromServer(String address, final String type, final String goal){
        HttpUtil.sendOkHttpRequest(address, new Callback() {
            @Override
            public void onResponse(Call call, Response response) throws IOException {
                String responseText=response.body().string();
                boolean result=false;
                if("province".equals(type)){
                    result= Utility.handleProvinceResponse(responseText);
                }else if("city".equals(type)){
                    result=Utility.handleCityResponse(responseText,selectedProvince.getId());
                }else if ("county".equals(type)){
                    result=Utility.handleCountyResponse(responseText,selectedCity.getId());
                }
                if(result){
                    if("province".equals(type)){
                        queryProvinces(goal);
                    }else if ("city".equals(type)){
                        queryCities(goal);
                    }else if("county".equals(type)){
                        queryCounties(goal);
                    }
                }
            }

            @Override
            public void onFailure(Call call, IOException e) {
               //donothing
            }
        });
    }
}