package com.ryanmuehe.maintenancerecords.configuration;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.springframework.stereotype.Service;

@Service
public class LogChangeConfig {

    public void changeLogLevel(String logPackage, String logLevel) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLogger(logPackage).setLevel(Level.valueOf(logLevel));
    }
}