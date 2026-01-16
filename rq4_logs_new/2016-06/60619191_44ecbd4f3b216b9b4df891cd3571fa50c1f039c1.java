package com.aeq;


import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jFileAppender {
	 static Logger logger = LoggerFactory.getLogger(Slf4jFileAppender.class);
     public static void main(String[] args) {
         for(int i=0; i<10; i++) {
             logger.debug("This is debug..");
             try {
                 Thread.sleep(new Random().nextInt(1000));
             } catch (InterruptedException e) {
                 logger.error("Error sleeping", e);
             }
             logger.info("simple logging ");
         }

     }
}