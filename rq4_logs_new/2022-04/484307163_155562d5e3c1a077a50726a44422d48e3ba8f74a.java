package com.andrey.currencyconverter.utils;

import com.andrey.currencyconverter.exceptions.RepositoryPathNotFoundException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ApplicationUtils {

    public static String getPathToJsonFileRepo() throws RepositoryPathNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties properties = new Properties();
        try (InputStream stream = loader.getResourceAsStream("application.properties")) {
            properties.load(stream);
            String pathToJsonFile = properties.getProperty("file-repository-path");

            if (pathToJsonFile.length() == 0) {
                throw new IOException();
            }

            return pathToJsonFile;
        } catch (IOException e) {
            throw new RepositoryPathNotFoundException("Property file not found or path to json file repo not found!");
        }
    }
}