package com.tomcat;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.jboss.jandex.Index;

import javax.servlet.ServletException;

/**
 * @Description
 * @Author admin
 * @Date 2020/4/18 16:51
 */
@Slf4j
public class AppTomcat {

    public static void main(String[] args) throws LifecycleException, ServletException {
        Tomcat tomcat=new Tomcat();
        tomcat.setPort(9070);
        tomcat.start();

        IndexServlet servlet=new IndexServlet();
        log.info("start the tomcat on prot:9070");
        Context context=tomcat.addWebapp("/app","d:\\temp\\");
        tomcat.addServlet("/app","index",servlet);
        context.addServletMappingDecoded("/index.do","index");
        tomcat.getServer().await();




    }

}