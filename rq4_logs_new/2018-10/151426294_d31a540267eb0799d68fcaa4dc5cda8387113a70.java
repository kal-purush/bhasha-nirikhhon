package com.example.zad02;

import com.example.zad02.service.GraphicsCardService;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        try {
            GraphicsCardService gpuService = new GraphicsCardService();
        }catch(SQLException e){
            e.printStackTrace();
        }
    }
}