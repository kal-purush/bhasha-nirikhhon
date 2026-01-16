package com.bank.model.service;

public class LogService {
    
    private static LogService logService;

    public static LogService getInstance(){
        if(logService == null){
            logService = new LogService();
        }
        return logService;
    }

    public void info(String message){
        log("[INFO]", message);
    }

    public void error(String message){
        log("[ERROR]", message);
    }

    private void log(String modePrefix, String message){
        System.out.println(modePrefix + " " + message);
    }
}