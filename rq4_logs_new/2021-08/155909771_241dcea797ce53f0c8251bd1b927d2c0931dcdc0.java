/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.unc.odum.dataverse.util.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author asone
 */
public class AppConfig {

    private static final Logger logger = Logger.getLogger(AppConfig.class.getName());

    public static final String TRSA_CONFIG_FILE_DIR = "trsa.configfile.directory";
    public static final String TRSA_CONFIG_FILE_NAME = "trsa.config";
    public static final String TRSA_FILES_DIR = "trsa.files.directory";

    String trsaFilesDirectory;

    public static void main(String[] args) {
        logger.log(Level.INFO, "AppConfig#main starts here");
        AppConfig appConfig = new AppConfig();
        appConfig.init();
        logger.log(Level.INFO, "AppConfig#main ends here");
    }

    public void init() {
        // take 1
        Map<String, String> configMap = readConfigFile();
        logger.log(Level.INFO, "take 1: configMap={0}", configMap);
        if (configMap.containsKey(TRSA_FILES_DIR)) {
            trsaFilesDirectory = configMap.get(TRSA_FILES_DIR);
            logger.log(Level.INFO, "trsaFilesDirectory={0}", trsaFilesDirectory);
        } else {
            logger.log(Level.INFO,
                    "key:TRSA_FILES_DIR={0} was not found in the trsa config file",
                    TRSA_FILES_DIR);
        }
        // take 2
        configMap = readConfigFile2();
        logger.log(Level.INFO, "take 2: configMap={0}", configMap);

    }

    private String getTrsaConfigFileDir() {
        Properties property = System.getProperties();
        String appConfigDir = property.getProperty(TRSA_CONFIG_FILE_DIR);
        logger.log(Level.INFO, "appConfigDir={0}", appConfigDir);

        return appConfigDir;
    }

    private Map<String, String> readConfigFile() {
        Map<String, String> stringStringMap = new ConcurrentHashMap<>();

        try (Stream<String> lines = Files.lines(Paths.get(getTrsaConfigFileDir() + "/" + TRSA_CONFIG_FILE_NAME))) {

            lines
                    .map(
                            s -> {
                                String[] splitStrings = s.split("\t", -1);
                                logger.log(Level.INFO, "splitStrings={0}", splitStrings);
                                stringStringMap.put(splitStrings[0], splitStrings[1]);
                                return stringStringMap;
                            }
                    ).count();

        } catch (IOException e) {
            logger.log(Level.WARNING, "IOException was thrown", e);
        }
        return stringStringMap;
    }

    private Map<String, String> readConfigFile2() {
        Map<String, String> stringStringMap = new ConcurrentHashMap<>();

        try (Stream<String> lines = Files.lines(Paths.get(getTrsaConfigFileDir()
                + "/" + TRSA_CONFIG_FILE_NAME))) {


            stringStringMap=lines
                    .map(
                            s -> {
                                String[] splitStrings = s.split("\t", -1);
                                logger.log(Level.INFO, "splitStrings={0}", splitStrings);
                                return splitStrings;
                            }
                    ).collect(Collectors.toMap(x->x[0], x->x[1]));


        } catch (IOException e) {
            logger.log(Level.WARNING, "IOException was thrown", e);
        }
        logger.log(Level.INFO, "splitStrings={0}", stringStringMap);
        return stringStringMap;
    }
}