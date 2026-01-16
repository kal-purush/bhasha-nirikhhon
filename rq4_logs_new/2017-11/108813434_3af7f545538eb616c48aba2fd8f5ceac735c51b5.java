package com.zcc.login.common.interceptor;

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
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import com.zcc.login.common.constant.CommonConstant;
import com.zcc.login.common.converter.UserConverter;
import com.zcc.login.common.utils.CookieUtil;
import com.zcc.login.common.utils.JwtTokenUtil;
import com.zcc.login.model.User;
import com.zcc.login.service.UserService;
import com.zcc.login.vo.SelectUserRequest;

/**
 * Created by ZhangChicheng on 2017/11/15.
 */
@Component
public class AuthenticationTokenInterceptor implements HandlerInterceptor {

	private final Logger logger = LoggerFactory.getLogger(AuthenticationTokenInterceptor.class);

	@Autowired
	private JwtTokenUtil jwtTokenUtil;
	@Autowired
	private UserService userService;
	@Autowired
	private UserConverter userConverter;

	@Override
	public boolean preHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
		Object o) throws Exception {
		String token = CookieUtil.getCookieValue(httpServletRequest, CommonConstant.X_ZCC_TOKEN);
		String userName = jwtTokenUtil.getUsernameFromToken(token);
		if (userName != null && SecurityContextHolder.getContext().getAuthentication() == null){
			User user = userService.getUser(userName);
			UserDetails userDetails = userConverter.covertUser2AuthUser(user);
			if(jwtTokenUtil.validateToken(token, userDetails)){
				UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(userDetails, null, userDetails.getAuthorities());
				authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(httpServletRequest));
				logger.info("authenticated user " + userName + ", setting security context");
				SecurityContextHolder.getContext().setAuthentication(authentication);
			}
		}
		return true;
	}

	@Override
	public void postHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object o,
		ModelAndView modelAndView) throws Exception {

	}

	@Override
	public void afterCompletion(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
		Object o, Exception e) throws Exception {

	}
}