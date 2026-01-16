package com.intetbanking.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ReadConfig {

    Properties pro;

    public ReadConfig()
    {
        File src= new File("./Configuration/config.properties");
        try
        {
            FileInputStream fis= new FileInputStream(src);
            pro= new Properties();
            pro.load(fis);

        }catch (Exception e)
        {
            System.out.print("Exception is"+e.getMessage());
        }
    }

    public String getApplicationURL()
    {
        String url=pro.getProperty("baseURL");
        return url;
    }
    public String getUserName()
    {
        String userName=pro.getProperty("username");
        return userName;
    }
    public String getPassword()
    {
        String Password=pro.getProperty("password");
        return Password;
    }
    public String getchromeDriverPath()
    {
        String chromeDriverPath=pro.getProperty("chromepath");
        return chromeDriverPath;
    }
    public String getIEDriverPath()
    {
        String IEDriverPath=pro.getProperty("iepath");
        return IEDriverPath;
    }
}