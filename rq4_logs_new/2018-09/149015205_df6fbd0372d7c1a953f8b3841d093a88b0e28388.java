package com.tdp2.quechuaapp.networking;

import com.tdp2.quechuaapp.model.Curso;

import java.util.ArrayList;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface DocenteApi {

    @GET("/public/cursos/{cursoID}")
    Call<Curso> getCurso(@Path("cursoId")Integer cursoId);
}