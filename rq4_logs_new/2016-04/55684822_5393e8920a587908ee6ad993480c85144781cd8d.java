package web.performance;

import java.io.IOException;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebFilter(filterName = "performanceFilter", urlPatterns = {"/*"})
public class PerformanceFilter implements Filter {
	private Logger log = LoggerFactory.getLogger(this.getClass());

	@Inject
	private PerformanceStatisticsBean performanceStatisticsBean;

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		HttpServletRequest httpRequest = (HttpServletRequest) request;
		HttpServletResponse httpResponse = (HttpServletResponse) response;
		
		long startTime = System.currentTimeMillis();
		
		try {
			chain.doFilter(httpRequest, httpResponse);
		} catch (Exception e) {
			log.error("Caught unhandled exception and set HTTP status code to 500 - INTERNAL_SERVER_ERROR", e);
			try {
				httpResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			} catch (IllegalStateException ex) {
				log.error("IllegalStateException: Error has been already sent?", ex);
			}
		}
		
		long endTime = System.currentTimeMillis();
		
		long time = endTime - startTime;
		performanceStatisticsBean.logTime(httpRequest.getMethod(), httpRequest.getRequestURI(), httpResponse.getStatus(), time);
	}

	@Override
	public void destroy() {
	}
}