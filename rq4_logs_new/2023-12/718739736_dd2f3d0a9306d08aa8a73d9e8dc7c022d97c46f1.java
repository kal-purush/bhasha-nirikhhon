package com.example.planties.data.scan.remote;

import com.example.planties.core.Constant;
import com.example.planties.core.jwt.TokenHandler;
import com.example.planties.data.plant.remote.network.PlantService;
import com.example.planties.data.scan.remote.dto.ScanRes;
import com.example.planties.data.scan.remote.network.ScanService;

import javax.inject.Inject;

import okhttp3.MultipartBody;
import retrofit2.Call;

public class ScanRemoteDataSourceImpl implements ScanRemoteDataSource {
    private final ScanService scanService;
    private final String AI_URL = Constant.API_AI + Constant.SCAN;
    @Inject
    public ScanRemoteDataSourceImpl(ScanService scanService) {
        this.scanService = scanService;
    }

    @Override
    public Call<ScanRes> scan(MultipartBody.Part file) {
        return scanService.scan(AI_URL, file);
    }
}