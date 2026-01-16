package com.spareparts.store;

import com.spareparts.store.controller.FrontController;
import com.spareparts.store.filter.AuthFilter;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.ContextConfig;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;

import java.io.File;
import java.util.Optional;

public class EmbeddedTomcatServer {
    public static final Optional<String> PORT = Optional.ofNullable(System.getenv("PORT"));
    public static final Optional<String> HOSTNAME = Optional.ofNullable(System.getenv("HOSTNAME"));

    public void start() {

        String contextPath = "/gymapp";
        String appBase = ".";
        Tomcat tomcat = new Tomcat();

        //define port, host, contextpath
        tomcat.setPort(Integer.valueOf(PORT.orElse("8080")));
        tomcat.setHostname(HOSTNAME.orElse("localhost"));
        tomcat.getHost().setAppBase(appBase);
        tomcat.addWebapp(contextPath, appBase);


        //annotation scanning
        Context context = tomcat.addContext("", new File(".").getAbsolutePath());
        context.addLifecycleListener(new ContextConfig());

        FilterDef myFilterDef = new FilterDef();
        myFilterDef.setFilterClass(AuthFilter.class.getName());
        myFilterDef.setFilterName(AuthFilter.class.getSimpleName());
        context.addFilterDef(myFilterDef);

        FilterMap filterMap = new FilterMap();
        filterMap.setFilterName(AuthFilter.class.getSimpleName());
        filterMap.addURLPattern("/*");
        context.addFilterMap(filterMap);

        Tomcat.addServlet(context, "FrontController", new FrontController());
        context.addServletMappingDecoded("/*", "FrontController");

        tomcat.getConnector();

        try {
            tomcat.start();
        } catch (LifecycleException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Server is running...");
        tomcat.getServer().await();
    }
}