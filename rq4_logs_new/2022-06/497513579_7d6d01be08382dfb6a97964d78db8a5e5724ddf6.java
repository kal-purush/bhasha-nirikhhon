package tests.properties;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;



public class SystemPropertiesTests {
    @Test
    void sameTests(){
        String browser = System.getProperty("browser");
        System.out.println(browser);
    }


    @Test
    void someTests2(){
        System.setProperty("browser", "safari");
        String browser = System.getProperty("browser");
        System.out.println(browser);
    }

    @Test
    void someTests3(){

        String browser = System.getProperty("browser","opera");
        System.out.println(browser);
    }

    @Test
    void someTests4(){
        System.setProperty("browser", "safari");
        String browser = System.getProperty("browser","opera");
        System.out.println(browser);
    }

    @Test
    void someTests5(){
        System.setProperty("anyValue", "any text");
        String browser = System.getProperty("anyValue","some text");
        System.out.println(browser);
    }


    @Test
    @Tag("test6")
    void someTests6(){
        String browser = System.getProperty("browser","chrome");
        String version = System.getProperty("version","101");
        String browserSize = System.getProperty("browserSize","1920x1080");
        System.out.println(browser);
        System.out.println(version);
        System.out.println(browserSize);
    }


    @Test
    @Tag("hello")
    void someTests7(){
        System.out.println("Hello " + System.getProperty("anyText"));

    }

}