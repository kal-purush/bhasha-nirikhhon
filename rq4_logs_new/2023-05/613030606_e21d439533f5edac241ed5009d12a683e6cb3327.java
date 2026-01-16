package ch.uzh.ifi.hase.soprafs23.constant;

import ch.uzh.ifi.hase.soprafs23.ServerCommandLineRunner;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

public class ShutdownServer {
    @Autowired
    ServerCommandLineRunner serverCommandLineRunner;


    /*@Test
    public void Shutdown(){
        serverCommandLineRunner.stopSocketServer();
    }*/
}