package br.com.raphael;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.ManagementSecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.security.SecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.web.ErrorMvcAutoConfiguration;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;


@Configuration
@EnableAutoConfiguration(exclude = { ErrorMvcAutoConfiguration.class, SecurityAutoConfiguration.class, ManagementSecurityAutoConfiguration.class })
@ComponentScan
@ImportResource({"spring/application-context.xml"})
public class Application {
	
	@Value("${service.path}")
    private String servicePath;

    @Bean
    public ServletRegistrationBean cxfServlet() {
        org.apache.cxf.transport.servlet.CXFServlet cxfServlet = new org.apache.cxf.transport.servlet.CXFServlet();
        ServletRegistrationBean servletDef = new ServletRegistrationBean(cxfServlet, "/" + servicePath + "/*");
        servletDef.setLoadOnStartup(1);
        return servletDef;
    }

	 public static void main(String[] args) {
	        // create application
	        SpringApplication app = new SpringApplication(Application.class);

	        // run application
	        app.run(args);
	    }

}