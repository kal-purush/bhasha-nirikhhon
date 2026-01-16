package com.zcc.login.common.interceptor;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import com.zcc.login.common.constant.CommonConstant;
import com.zcc.login.common.converter.UserConverter;
import com.zcc.login.common.utils.CookieUtil;
import com.zcc.login.common.utils.JwtTokenUtil;
import com.zcc.login.model.User;
import com.zcc.login.service.UserService;

/**
 * Created by ZhangChicheng on 2017/11/20.
 */
@Component
public class AuthenticationTokenFilter extends OncePerRequestFilter {
	private final Logger logger = LoggerFactory.getLogger(AuthenticationTokenFilter.class);

	@Autowired
	private JwtTokenUtil jwtTokenUtil;
	@Autowired
	private UserService userService;
	@Autowired
	private UserConverter userConverter;
	@Override
	protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
		FilterChain filterChain) throws ServletException, IOException {
		String token = CookieUtil.getCookieValue(httpServletRequest, CommonConstant.X_ZCC_TOKEN);
		String userName = jwtTokenUtil.getUsernameFromToken(token);
		if (userName != null && SecurityContextHolder.getContext().getAuthentication() == null){
			User user = userService.getUser(userName);
			UserDetails userDetails = userConverter.covertUser2AuthUser(user);
			if(jwtTokenUtil.validateToken(token, userDetails)){
				UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(userDetails, "", userDetails.getAuthorities());
				authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(httpServletRequest));
				logger.info("authenticated user " + userName + ", setting security context");
				SecurityContextHolder.getContext().setAuthentication(authentication);
				httpServletRequest.setAttribute("user", user);
			}
		}
		filterChain.doFilter(httpServletRequest, httpServletResponse);
	}
}