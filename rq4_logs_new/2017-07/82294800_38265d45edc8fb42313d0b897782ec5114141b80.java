package es.hiiberia.simpatico.filters;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import org.apache.log4j.Logger;

import es.hiiberia.simpatico.rest.SimpaticoResourceUtils;
import es.hiiberia.simpatico.utils.SimpaticoProperties;


public class AuthenticationFilter implements Filter {
	
   public void init(FilterConfig filterConfig) throws ServletException {
	   Logger.getRootLogger().info("INIT SIMPATICO AUTHENTICATION FILTER");		

   }

   public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
	   
	   String ipClient = request.getRemoteAddr();
	   Logger.getRootLogger().info("[Auth filter] IP request: " + ipClient);		
       HttpServletRequest httpRequest = (HttpServletRequest) request;   
       
       //Logger.getRootLogger().debug(SimpaticoResourceUtils.getHeaders(httpRequest));   // Print all headers
       
       if (SimpaticoProperties.aacUse) {
    	   
    	   // Check allowed IPs
    	   String proxyRealIp;
           if ((proxyRealIp = SimpaticoResourceUtils.getRealIPHeader(httpRequest)) != null) { // If null -> Not using proxy (or header in simpatico.properties dont match)
        	   Logger.getRootLogger().info("[Auth filter] Looks like there is a proxy server. Changing IP (" + ipClient + ") to 'real ip header' (" + proxyRealIp + ")");
        	   ipClient = proxyRealIp;
           }
           if (SimpaticoProperties.ipsAllowed.contains(ipClient)) {
    		   Logger.getRootLogger().debug("[Auth filter] IP exists in whitelist. Method: " + httpRequest.getMethod());
    		   filterChain.doFilter(request, response);
    		   return;
    	   } else {
    		   Logger.getRootLogger().debug("[Auth filter] IP doesnt exists in whitelist. IP: " + ipClient + ". Method: " + httpRequest.getMethod());
    	   }
    	   
    	   Logger.getRootLogger().info("[Auth filter] Using filter. Method: " + httpRequest.getMethod());
    	   if (httpRequest.getMethod().equalsIgnoreCase("GET")) {
        	   // User/Pass base64 compare
    		   String authRequest = httpRequest.getHeader("Authorization");
    		   Logger.getRootLogger().debug("[Auth filter] AuthRequest: " + authRequest);
			   
    		   if (authRequest != null) {
    			   String [] splitAuthRequest = authRequest.split(" ");  // AuthRequest = Basic <token>
    			   if (splitAuthRequest.length >= 2) {
    				   String basicAuth = SimpaticoProperties.aacGetAuthUser + ":" + SimpaticoProperties.aacGetAuthPass;
    				   byte[] bytes = basicAuth.getBytes("UTF-8");
    				   String encoded = Base64.getEncoder().encodeToString(bytes);
    				   
    				   Logger.getRootLogger().info("[Auth filter]  My user/pass to encode: " + basicAuth + ". Encoded: " + encoded + ". Recv: " + splitAuthRequest[1]);
    				   if (encoded.equals(splitAuthRequest[1])) {
    					   filterChain.doFilter(request, response);
    	        		   return;
    				   } else {
    					   Logger.getRootLogger().info("[Auth filter] Basic auth dont match (" + encoded + " recv: " + splitAuthRequest[1]);
    				   }
    			   }
    		   }
           } else { // POST/PUT/DELETE            	   
        	   // Do Get request to know if token is valid
        	   String url = SimpaticoProperties.aacUrlServer + SimpaticoProperties.aacPathServer + "?scope";
        	   
        	   URL obj = new URL(url);
        	   HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        	   
        	   // optional default is GET
        	   con.setRequestMethod("GET");
        	   
        	   //add request header
        	   con.setRequestProperty("User-Agent", httpRequest.getHeader("User-Agent"));
        	   con.setRequestProperty("Content-Type", "application/json");
        	   con.setRequestProperty("Authorization", httpRequest.getHeader("Authorization")); 
        	   
        	   int responseCode = con.getResponseCode();
        	   Logger.getRootLogger().debug("[Auth filter] Sending 'GET' request to URL : " + url + " with token: " + httpRequest.getHeader("Authorization"));
        	   Logger.getRootLogger().debug("[Auth filter] Response Code : " + responseCode);
        	 
        	   BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        	   String inputLine;
        	   StringBuffer responseHttp = new StringBuffer();
    			
        	   while ((inputLine = in.readLine()) != null) {
        		   responseHttp.append(inputLine);
        	   }
        	   in.close();
    			
        	   //print result
        	   Logger.getRootLogger().debug("[Auth filter] Response: " + responseHttp.toString());
        	   
        	   if (responseCode == 200 && responseHttp.toString().equalsIgnoreCase("true")) {
        		   Logger.getRootLogger().debug("[Auth filter] Token authorized");
        		   filterChain.doFilter(request, response);
        		   return;
        	   } else {
        		   Logger.getRootLogger().debug("[Auth filter] Token unauthorized. response code: " + responseCode + ". Response page: " + responseHttp.toString());
        	   }
           }
    	   
    	   response.resetBuffer();
   		   response.getOutputStream().write("{\"message\": \"Access Denied\"}".getBytes());
   		   HttpServletResponse hsr = (HttpServletResponse) response;
   		   hsr.setStatus(403);
   		   return;
       } else {
    	   filterChain.doFilter(request, response);
       }
   }

   public void destroy() {
	   Logger.getRootLogger().info("DESTROY SIMPATICO AUTHENTICATION FILTER");		
   }
}