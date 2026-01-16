package cn.edu.tit.interceptor;

import java.io.OutputStream;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

public class CsrfInterceptor implements HandlerInterceptor {
	/**
	 * This implementation always returns {@code true}.
	 * @author wenli
	 * service之前执行
	 * 用于登录之前验证_csrf参数
	 */
	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws Exception {
		HttpSession session = request.getSession();
		String _csrfByForm = request.getParameter("_csrf");//表单csrf
		String _csrfBySession = String.valueOf(session.getAttribute("_csrf"));//session中的csr
		Object obj = session.getAttribute("teacherId"); //当前teacherid
		System.out.println("到达拦截器"+_csrfByForm+"****"+_csrfBySession);
       
            //移除当前session中的csrf,防止连续登录
		session.removeAttribute("_csrf");
		//验证是否存在CSRF攻击
		if(StringUtils.isNotBlank(_csrfByForm)&&StringUtils.isNotBlank(_csrfBySession)&&_csrfByForm.equals(_csrfBySession)) {
			return true;
		}    else {
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(403);
			//页面友好提示
			OutputStream oStream = response.getOutputStream();
			oStream.write("请不要重复提交请求，返回原始页面刷新后再次尝试！！！".getBytes("UTF-8"));
			return false;
		}
	}

	/**
	 * This implementation is empty.
	 * 在controller执行之后的service之后执行
	 */
	@Override
	public void postHandle(
			HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView)
			throws Exception {
	}

	/**
	 * This implementation is empty.
	 * 在页面渲染完成返回给客户端之前执行
	 */
	@Override
	public void afterCompletion(
			HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
			throws Exception {
		 
	}

	

}