package com.project.Link;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;
import org.springframework.web.servlet.support.RequestContextUtils;

import com.project.Link.Noticement.Controller.NoticementControllerImple;

/* Intercepter:
 * 출처: https://rongscodinghistory.tistory.com/2 [악덕고용주의 개발 일기]
*/
public class SessionControlInterceptor extends HandlerInterceptorAdapter{
	
	private static final Logger logger = LoggerFactory.getLogger(NoticementControllerImple.class);
	
	public SessionControlInterceptor(){};
	
	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws Exception {
		logger.info("::Prehandle requested");
		
		HttpSession session = request.getSession();
		HashMap<String, String> sr = new HashMap<String, String>();
		Map<String, ?> redirectAttrs = RequestContextUtils.getInputFlashMap(request);
		if(redirectAttrs!=null) {
			logger.info("/////////////////////////FlashInfos Extracted////////");
			session.setAttribute("usrId", (String)redirectAttrs.get("usrId"));
			session.setAttribute("isAdmin", (String)redirectAttrs.get("isAdmin"));
			logger.info("//At Redirective Access//SessionControlIntercepter got this :::: // ID : " + session.getAttribute("usrId") + "isAdmin : " +session.getAttribute("isAdmin"));
			return true;
		}else if(session.getAttribute("usrId") != null) {
			logger.info("///////////////////////sessioninfo Confirmed////////////");
			logger.info("//At regular Access///SessionControlIntercepter got this :::: // ID : " + session.getAttribute("usrId") + "isAdmin : " +session.getAttribute("isAdmin"));
			return true;
			
		}else {
			logger.info("////////////////Denied!////////////////////");
			response.sendRedirect("/Link/");
			return true;
		}
	
		
		/*
		 * // TODO Auto-generated method stub return super.preHandle(request, response,
		 * handler);
		 */
	}

	@Override
	public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
			ModelAndView modelAndView) throws Exception {
		// TODO Auto-generated method stub
		super.postHandle(request, response, handler, modelAndView);
	}

	@Override
	public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
			throws Exception {
		// TODO Auto-generated method stub
		super.afterCompletion(request, response, handler, ex);
	}

}