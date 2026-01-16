package com.example.planties.data.garden.remote;

import com.example.planties.core.jwt.TokenHandler;
import com.example.planties.core.utils.NetworkUtil;
import com.example.planties.data.garden.remote.dto.GardenDetailRes;
import com.example.planties.data.garden.remote.dto.GardenReq;
import com.example.planties.data.garden.remote.dto.GardenListRes;
import com.example.planties.data.garden.remote.network.GardenService;

import javax.inject.Inject;

import retrofit2.Call;

public class GardenRemoteDataSourceImpl implements GardenRemoteDataSource{
    private final GardenService gardenService;
    private final TokenHandler tokenHandler;

    @Inject
    public GardenRemoteDataSourceImpl(GardenService gardenService, TokenHandler tokenHandler){
        this.gardenService = gardenService;
        this.tokenHandler = tokenHandler;
    }
    @Override
    public Call<GardenListRes> getGarden() {
        return gardenService.getGarden(NetworkUtil.getAuthHeader(tokenHandler.getAccessToken())); //SharedPreference
    }

    @Override
    public Call<GardenDetailRes> postGarden(GardenReq gardenReq){
        return gardenService.postGarden(gardenReq, NetworkUtil.getAuthHeader(tokenHandler.getAccessToken()));
    }

    @Override
    public Call<GardenDetailRes> getDetailGarden(String gardenId) {
        return gardenService.getDetailGarden(gardenId, NetworkUtil.getAuthHeader(tokenHandler.getAccessToken()));
    }

    @Override
    public Call<GardenDetailRes> putGarden(String gardenId, GardenReq gardenReq) {
        return gardenService.putGarden(gardenId, gardenReq, NetworkUtil.getAuthHeader(tokenHandler.getAccessToken()));
    }

    @Override
    public Call<GardenDetailRes> deleteGarden(String gardenId) {
        return gardenService.deleteGarden(gardenId, NetworkUtil.getAuthHeader(tokenHandler.getAccessToken()));
    }
}