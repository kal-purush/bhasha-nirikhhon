package config.initializer;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration.Dynamic;

import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.support.HttpRequestHandlerServlet;

/**
 * 
 * ResourceInitializer Servlet3.0 工程入口类
 *
 */
public class ResourceInitializer implements WebApplicationInitializer {

    public void onStartup(ServletContext servletContext) throws ServletException {
        HttpRequestHandlerServlet httpRequestHandlerServlet = new HttpRequestHandlerServlet();
        Dynamic registration = servletContext.addServlet("defaultServlet", httpRequestHandlerServlet);
        registration.setLoadOnStartup(1);
        registration.addMapping(new String[] { "/resource/*", "/favicon.ico" });
        HttpRequestHandlerServlet siteHttpRequestHandlerServlet = new HttpRequestHandlerServlet();
        Dynamic webRegistration = servletContext.addServlet("webServlet", siteHttpRequestHandlerServlet);
        webRegistration.setLoadOnStartup(0);
        webRegistration.addMapping(new String[] { "/web/*" });
    }
}