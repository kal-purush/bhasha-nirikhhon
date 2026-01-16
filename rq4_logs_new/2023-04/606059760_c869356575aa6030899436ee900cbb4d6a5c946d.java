package com.mggcode.gestion_bd_elecciones.config;

import java.io.FileInputStream;
import java.util.Properties;

public class Config {
    private static Config configuracion;
    public static Properties config;

    private Config() {
        config = new Properties();
        loadConfig();
    }

    public static Config getConfiguracion() {
        if (configuracion == null) {
            configuracion = new Config();
        }
        return configuracion;
    }

    public void loadConfig() {
        try {
            config.load(new FileInputStream("src/main/resources/config.properties"));
        } catch (Exception e) {
            System.out.println("Error cargando configuración");
        }
    }
    public static String getIpDbPrincipal(){return config.getProperty("ipPrincipal");}
    public static String getIpDbReserva(){return config.getProperty("ipReserva");}
    public static String getRutaFichero(){return config.getProperty("rutaFichero");}
}