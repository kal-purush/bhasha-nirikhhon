package com.ss.ita.econews.ui;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class CredentialProperties {
    FileInputStream fis;
    Properties prop;

    public CredentialProperties() {
        try {
            this.fis = new FileInputStream("src/test/resources/credential.properties");
            this.prop = new Properties();
            prop.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getTarasKrasitskyiLogin() {
        return prop.getProperty("TARAS_KRASITSKYI_LOGIN");
    }
    public String getTarasKrasitskyiPassword() {
        return prop.getProperty("TARAS_KRASITSKYI_PASS");
    }

    public String getTestUserLogin() {
        return prop.getProperty("VLAD_DMYTRIV_LOGIN");
    }
    public String getTestUserPassword() {
        return prop.getProperty("TEST_USER_PASS");
    }

    public String getVladDmytrivLogin() { return prop.getProperty("VLAD_DMYTRIV_LOGIN"); }
    public String getVladDmytrivPassword() { return prop.getProperty("VLAD_DMYTRIV_PASS"); }

    public String getTestUserGC871Login() { return prop.getProperty("TEST_USER_GC_871_LOGIN"); }
    public String getTestUserGC871Password() { return prop.getProperty("TEST_USER_GC_871_PASS"); }

}