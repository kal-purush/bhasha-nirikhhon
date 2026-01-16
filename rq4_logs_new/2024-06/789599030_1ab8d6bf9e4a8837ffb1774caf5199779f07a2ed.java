package com.moviezone.dai_api.utils;

public class CertUtil {
    public static String convertCert(String cert){
        switch (cert){
            case "G" -> {
                return "ATP";
            }
            case "PG" -> {
                return "+13";
            }
            case "PG-13" -> {
                return "+16";
            }
            case "R" -> {
                return "+18";
            }
            case "NC-17" -> {
                return "+18";
            }
            case "" -> {
                return "ATP";
            }
        }
        return null;
    }
}