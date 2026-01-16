package edu.iss.gitcloneteam.weatherobserver.utils;

import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

@Component
public class ProjectVersionReader {

    public final static String DEFAULT_VERSION = "0.0.0-unspecified";

    public String readVersion() {

        BufferedReader bufferedReader;
        String currentVersion = null;
        try {
            bufferedReader = new BufferedReader(new FileReader("classpath:/git_version.txt"));
            currentVersion = bufferedReader.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return currentVersion;
    }

}