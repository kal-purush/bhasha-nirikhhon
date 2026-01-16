package com.example.planties.data.source.remote;

import com.example.planties.data.source.remote.dto.AuthRequest;
import com.example.planties.data.source.remote.dto.AuthResponse;
import com.example.planties.data.source.remote.network.AuthService;

import retrofit2.Call;

public class AuthRemoteDataSourceImpl implements AuthRemoteDataSource{
    private final AuthService authService;
    public AuthRemoteDataSourceImpl(AuthService authService){
        this.authService = authService;
    }
    @Override
    public Call<AuthResponse> postLogin(AuthRequest authRequest) {
        return authService.postLogin(authRequest);
    }
}