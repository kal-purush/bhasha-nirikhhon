package com.webvidhi.pubsub.config;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

public class JwtAuthFilter extends OncePerRequestFilter{
	
	@Value("${jwt.header}")
	String header;
	
	@Value("${jwt.prefix}")
	String prefix;
	
	JwtUtil jwtUtil = new JwtUtil();

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
			throws ServletException, IOException {
		
		System.out.println("Filter hit!!");
		
		//Get headers
		String authHeader = request.getHeader("Authorization");
		
		// get bearer token
	    if( authHeader != null  && authHeader.startsWith("Bearer")) {
	    	
	    	System.out.println("header: "+authHeader );
	    	try {
	    		String token = authHeader.replace("Bearer ", "");
	    		System.out.println("Token is:"+ token);
	    		String userName = jwtUtil.getUserName(token);
	    		System.out.println("userName: "+userName);
	    		
	    		if( userName != null) {
	    			System.out.println("Valid user..");

					//List<String> authorities = (List<String>) jwtUtil.getClaimsfromToken(token).get("authorities");
	    			 List<GrantedAuthority> grantedAuths =
	    		                AuthorityUtils.commaSeparatedStringToAuthorityList("ADMIN");
					
					// UsernamePasswordAuthenticationToken: A built-in object, used by spring to represent the current authenticated / being authenticated user.
					// It needs a list of authorities, which has type of GrantedAuthority interface, where SimpleGrantedAuthority is an implementation of that interface
					 UsernamePasswordAuthenticationToken auth = new UsernamePasswordAuthenticationToken(
							 userName, null, grantedAuths);
					 
					 // 6. Authenticate the user
					 // Now, user is authenticated
					 SecurityContextHolder.getContext().setAuthentication(auth);
				}
	    		else
	    		{
System.out.println("Error!");
	    		}
	    	}
	    	catch(Exception exception){
	    		System.out.println("Error exception!"+exception.getLocalizedMessage());
	    		//SecurityContextHolder
	    		SecurityContextHolder.clearContext();
	    		
	    	}
	    }
	    filterChain.doFilter(request, response);
	}

}