package org.springframework.issues.config;

import javax.servlet.Filter;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.filter.DelegatingFilterProxy;
import org.springframework.web.filter.HiddenHttpMethodFilter;
import org.springframework.web.filter.ShallowEtagHeaderFilter;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;

public class AppInitializer  extends AbstractAnnotationConfigDispatcherServletInitializer{

	public AppInitializer() {
		
	}
	
	@Override
	public void onStartup(ServletContext servletContext) throws ServletException {
		
		CharacterEncodingFilter characterEncoding = new CharacterEncodingFilter();
		characterEncoding.setEncoding("UTF-8");
		characterEncoding.setForceEncoding(true);
		
		registerServletFilter(servletContext, characterEncoding);
		
		super.onStartup(servletContext);
		
	}

	@Override
	protected WebApplicationContext createRootApplicationContext() {
		return super.createRootApplicationContext();
	}

	@Override
	protected Class<?>[] getRootConfigClasses() {
		return null;
	}

	@Override
	protected Class<?>[] getServletConfigClasses() {
		return new Class<?>[]{WebConfig.class};
	}

	@Override
	protected String[] getServletMappings() {
		return new String[]{"/"};
	}
	
	@Override
	protected Filter[] getServletFilters() {
		
		return new Filter[]{
//			new GzipFilter(),
//			new SiteMeshFilter(),
			
		};
	}
}