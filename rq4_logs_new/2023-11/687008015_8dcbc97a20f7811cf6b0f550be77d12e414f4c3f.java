package com.sharework.config;

import com.google.firebase.FirebaseApp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ResourceLoader;

@SpringBootTest
class FirebaseConfigTest {

    @Autowired
    FirebaseConfig firebaseConfig;

    @Autowired
    ResourceLoader resourceLoader;

    @Test
    void initializeFirebase() {
        FirebaseApp firebaseApp = firebaseConfig.initializeFirebase(resourceLoader);
        Assertions.assertEquals("[DEFAULT]", firebaseApp.getName());
    }
}