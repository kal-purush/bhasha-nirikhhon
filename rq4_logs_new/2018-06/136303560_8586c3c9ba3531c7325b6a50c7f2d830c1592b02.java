/**
 * BEYONDSOFT.COM INC
 */
package cn.liusk.redistest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * @author liusk
 * @version $Id: StartApplication.java, v 0.1 2018/6/7 9:36 liusk Exp $
 */
@SpringBootApplication
public class StartApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(StartApplication.class,args);
    }

}