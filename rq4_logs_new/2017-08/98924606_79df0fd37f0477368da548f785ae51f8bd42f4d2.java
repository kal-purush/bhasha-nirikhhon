package com.example.brahma.coolweather.presenter;

import com.example.brahma.coolweather.db.City;
import com.example.brahma.coolweather.db.County;
import com.example.brahma.coolweather.db.Province;
import com.example.brahma.coolweather.util.HttpUtil;
import com.example.brahma.coolweather.util.Utility;

import org.litepal.crud.DataSupport;

import java.io.IOException;
import java.util.List;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Response;

/**
 * Created by 10750 on 2017/8/3.
 */

public class AreaPresenter implements IAreaContact.IAreaPresenter {

    private IAreaContact.IAreaView mAreaView;

    public AreaPresenter(IAreaContact.IAreaView areaView){
        mAreaView=areaView;
    }
    @Override
    public void getProvinces() {
        List<Province> provinceList= DataSupport.findAll(Province.class);
        if(provinceList.size()!=0){
            mAreaView.showProvinces(provinceList);
        }else{
            HttpUtil.sendOkHttpRequest("http://guolin.tech/api/china", new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    mAreaView.showFailure("加载失败："+e.getMessage());
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    mAreaView.showProvinces(Utility.handleProvinceResponse(response.body().string()));
                }
            });
        }
    }

    @Override
    public void getCities(final Province province) {
        final List<City> cityList= DataSupport.where("provinceId=?",province.getProvinceId()+"").find(City.class);
        if(cityList.size()!=0){
            mAreaView.showCities(cityList);
        }else{
            HttpUtil.sendOkHttpRequest("http://guolin.tech/api/china/"+province.getProvinceId(), new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    mAreaView.showFailure("加载失败："+e.getMessage());
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    List<City> cityList1=Utility.handleCityResponse(response.body().string(),province.getProvinceId());
                    mAreaView.showCities(cityList1);
                }
            });
        }
    }

    @Override
    public void getCounties(final City city) {
        List<County> countyList= DataSupport.where("cityId=?",city.getCityId()+"").find(County.class);
        if(countyList.size()!=0){
            mAreaView.showCounties(countyList);
        }else{
            HttpUtil.sendOkHttpRequest("http://guolin.tech/api/china/"+city.getProvinceId()+"/"+city.getCityId(), new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    mAreaView.showFailure("加载失败："+e.getMessage());
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    List<County> countyList1=Utility.handleCountyResponse(response.body().string(),city.getCityId());
                    mAreaView.showCounties(countyList1);
                }
            });
        }
    }
}