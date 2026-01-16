package com.example.multithreadingProject.LoggerSystem;

import com.example.multithreadingProject.LoggerSystem.classes.DebugLogProcessor;
import com.example.multithreadingProject.LoggerSystem.classes.ErrorLogProcessor;
import com.example.multithreadingProject.LoggerSystem.classes.InfoLogProcessor;
import com.example.multithreadingProject.LoggerSystem.classes.LogProcessor;

public class LoggingApplication {
    public static void main(String[] args) {
        LogProcessor logProcessor = new InfoLogProcessor(new DebugLogProcessor(new ErrorLogProcessor(null)));

        logProcessor.log(LogProcessor.DEBUG, "Debug msg");
        logProcessor.log(LogProcessor.INFO, "Info msg");
        logProcessor.log(LogProcessor.ERROR, "Error msg");
    }
}