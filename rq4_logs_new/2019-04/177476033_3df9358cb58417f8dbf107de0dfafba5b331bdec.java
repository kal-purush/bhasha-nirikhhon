package servlet;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.junit.Test;
import org.mockito.Mockito;

public class LoginServletTest {
	
	@Test
	//correct user test
	public void testServlet1() throws Exception {
		ServletConfig sg = mock(ServletConfig.class);

		HttpServletRequest request = mock(HttpServletRequest.class);       
        when(request.getParameter("username")).thenReturn("nero");
        when(request.getParameter("pw")).thenReturn("domusaurea");
        Mockito.doNothing().when(request).setAttribute(any(), any());

        HttpServletResponse response = mock(HttpServletResponse.class);
        Mockito.doNothing().when(response).setContentType(any());
        
        RequestDispatcher dispatch = mock(RequestDispatcher.class);
        Mockito.doNothing().when(dispatch).forward(any(), any());
        
        ServletContext servletContext = mock(ServletContext.class);
        when(servletContext.getRequestDispatcher(any())).thenReturn(dispatch);
        LoginServlet servlet = new LoginServlet() {
        	public ServletContext getServletContext() {
        	      return servletContext; // return the mock
        	    }
        };
        servlet.init(sg);
        servlet.service(request, response);
        Mockito.verify(request, Mockito.times(0)).setAttribute(any(),any());
	}
	
	@Test
	//wrong user test
	public void testServlet2() throws Exception {
		ServletConfig sg = mock(ServletConfig.class);

		HttpServletRequest request = mock(HttpServletRequest.class);       
        when(request.getParameter("username")).thenReturn("julius");
        when(request.getParameter("pw")).thenReturn("domusaurea");
        Mockito.doNothing().when(request).setAttribute(any(), any());

        HttpServletResponse response = mock(HttpServletResponse.class);
        Mockito.doNothing().when(response).setContentType(any());
        
        RequestDispatcher dispatch = mock(RequestDispatcher.class);
        Mockito.doNothing().when(dispatch).forward(any(), any());
        
        ServletContext servletContext = mock(ServletContext.class);
        when(servletContext.getRequestDispatcher(any())).thenReturn(dispatch);
        LoginServlet servlet = new LoginServlet() {
        	public ServletContext getServletContext() {
        	      return servletContext; // return the mock
        	    }
        };
        servlet.init(sg);
        servlet.service(request, response);
        Mockito.verify(request, Mockito.times(2)).setAttribute(any(),any());
	}
	
	@Test
	//empty user test
	public void testServlet3() throws Exception {
		ServletConfig sg = mock(ServletConfig.class);

		HttpServletRequest request = mock(HttpServletRequest.class);       
        when(request.getParameter("username")).thenReturn("");
        when(request.getParameter("pw")).thenReturn("domusaurea");
        Mockito.doNothing().when(request).setAttribute(any(), any());

        HttpServletResponse response = mock(HttpServletResponse.class);
        Mockito.doNothing().when(response).setContentType(any());
        
        RequestDispatcher dispatch = mock(RequestDispatcher.class);
        Mockito.doNothing().when(dispatch).forward(any(), any());
        
        ServletContext servletContext = mock(ServletContext.class);
        when(servletContext.getRequestDispatcher(any())).thenReturn(dispatch);
        LoginServlet servlet = new LoginServlet() {
        	public ServletContext getServletContext() {
        	      return servletContext; // return the mock
        	    }
        };
        servlet.init(sg);
        servlet.service(request, response);
        Mockito.verify(request, Mockito.times(1)).setAttribute(any(),any());
	}
	
	@Test
	//empty password test
	public void testServlet4() throws Exception {
		ServletConfig sg = mock(ServletConfig.class);

		HttpServletRequest request = mock(HttpServletRequest.class);       
        when(request.getParameter("username")).thenReturn("nero");
        when(request.getParameter("pw")).thenReturn("");
        Mockito.doNothing().when(request).setAttribute(any(), any());

        HttpServletResponse response = mock(HttpServletResponse.class);
        Mockito.doNothing().when(response).setContentType(any());
        
        RequestDispatcher dispatch = mock(RequestDispatcher.class);
        Mockito.doNothing().when(dispatch).forward(any(), any());
        
        ServletContext servletContext = mock(ServletContext.class);
        when(servletContext.getRequestDispatcher(any())).thenReturn(dispatch);
        LoginServlet servlet = new LoginServlet() {
        	public ServletContext getServletContext() {
        	      return servletContext; // return the mock
        	    }
        };
        servlet.init(sg);
        servlet.service(request, response);
        Mockito.verify(request, Mockito.times(1)).setAttribute(any(),any());
	}
}