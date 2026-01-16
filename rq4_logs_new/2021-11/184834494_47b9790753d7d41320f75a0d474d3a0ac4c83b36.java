/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.task.fw;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * @author dirceubelem
 */
public class ConfigParam {

    public static boolean production = true;

    private PropertiesConfiguration properties;
    private static ConfigParam instance;
    public static String PATH =
            System.getProperty("user.home").toLowerCase() + File.separator + "taskapi" + File.separator;

    private static final String CONFIG_PATH = "config.properties";

    private ConfigParam() throws ConfigurationException, IOException {

        File file = new File(PATH);
        if (!file.exists()) {
            file.mkdir();
        }

        String configParamPath;
        file = new File(PATH + CONFIG_PATH);
        if (!file.exists()) {
            String filePath = ConfigParam.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            try {
                filePath = URLDecoder.decode(filePath, "UTF-8");
            } catch (UnsupportedEncodingException ex) {
                filePath = filePath.replace("%20", " ");
            }

            File jarfile = new File(filePath);
            File libFile = jarfile.getParentFile();
            File webinfFile = libFile.getParentFile();

            for (File childFile : webinfFile.listFiles()) {
                if (childFile.getName().equals(CONFIG_PATH)) {
                    FileUtils.copyFile(childFile, file);
                    break;
                }
            }
        }

        configParamPath = file.getPath();

        if (configParamPath != null) {
            this.properties = new PropertiesConfiguration(configParamPath);
            this.properties.setAutoSave(true);
            this.properties.load();
        }
    }

    // REDO: PropertiesConfiguration possui refresh e reload, não faz sentido recarregar o arquivo dessa forma
    public static void renewProperties() throws ConfigurationException, IOException {
        String configParamPath = PATH + CONFIG_PATH;

        PropertiesConfiguration properties = new PropertiesConfiguration(configParamPath);
        properties.setAutoSave(true);

        properties.load();
        getConfigParam().setProperties(properties);
    }

    public void setProperties(PropertiesConfiguration properties) {
        this.properties = properties;
    }

    public static ConfigParam getConfigParam() throws ConfigurationException, IOException {

        if (instance == null) {
            instance = new ConfigParam();
        }
        return instance;
    }

    public String getProperties(String param) {
        return instance.properties.getString(param);
    }

    public void setProperties(String key, String param) {
        instance.properties.setProperty(key, param);
    }

}