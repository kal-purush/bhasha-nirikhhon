package hibernate.controller;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

public class ApplicationConfig implements WebApplicationInitializer{

	@Override
	public void onStartup(ServletContext servletContext) throws ServletException {
		
	// change this to code	
	/*<servlet>
	      <servlet-name>SpringMVC_Hibernate4</servlet-name>
	      <servlet-class>
	         org.springframework.web.servlet.DispatcherServlet
	      </servlet-class>
	  	 <init-param>
	        <param-name>contextConfigLocation</param-name>
	        <param-value>/WEB-INF/spring/appServlet/servlet-context.xml</param-value>
	    </init-param>
	    <load-on-startup>1</load-on-startup>
	    <servlet-mapping>
      		<servlet-name>SpringMVC_Hibernate4</servlet-name>
      		<url-pattern>/</url-pattern>
   		</servlet-mapping>
 
	 </servlet>*/
		
		
		AnnotationConfigWebApplicationContext appContext = new AnnotationConfigWebApplicationContext();
        appContext.register(ApplicationContextConfig.class);
         
        ServletRegistration.Dynamic dispatcher = servletContext.addServlet( "SpringDispatcher", new DispatcherServlet(appContext));
        dispatcher.setLoadOnStartup(1);
        dispatcher.addMapping("/");
        
        
	}

}