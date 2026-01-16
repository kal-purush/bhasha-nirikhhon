package com.pl.isolution.automatization;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            while (System.in.available() == 0) {
                TimeUnit.MILLISECONDS.sleep(500);
                logger.debug("Siema {}, {}", "arg1", "arg2");
            }
        }catch(Exception e){
            logger.debug("Error {}, {}", "arg1", "arg2");
        }
    }
}