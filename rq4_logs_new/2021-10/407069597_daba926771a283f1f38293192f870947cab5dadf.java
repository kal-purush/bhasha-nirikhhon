package com.bithumb.interest.controller;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;


class S3ConnectionTest {
    private final String DRIVER = "org.mariadb.jdbc.Driver";
    private final String URL = "jdbc:mariadb://youngcha-interest.crtppt0lvgk7.ap-northeast-2.rds.amazonaws.com:3306/bithumb";
    private final String USER ="admin";
    private final String PW = "youngcha1008";

    @Test
    @DisplayName("AWS Connection Test")
    void connectionTest() {
        try {
            Class.forName(DRIVER);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection con = DriverManager.getConnection(URL, USER, PW)){
            System.out.println(con);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}