package com.the.controller;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.the.controller.blood.BloodController;
import com.the.controller.movie.MovieController;
import com.the.model.blood.BloodService;
import com.the.model.movie.MovieService;

/*
 * 	클라이언트의 요청마다, 해다 요청을 처리하는 컨트롤러를 일일이
 * 매핑하면, 유지보수성이 떨어지고 업무 비효율적이므로
 * 클라이언트의 요청은 일단 하나의 진입점으로 몰아서 처리해야 한다..
 * (대형 어플리케이션..)
 * */
public class DispatcherServlet extends HttpServlet {
	Properties props;
	FileInputStream fis = null;

	// 서블릿이 인스턴스화 될때, 컨테이너로부터 호출되는 초기화 메서드!!
	// 따라서 개발자는 init()에 초기화 작업 코드를 작성해 넣으면 된다!!
	@Override
	public void init(ServletConfig config) throws ServletException {
		//어플리케이션의 전역적 정보를 갖고 있기 때문에, 웹어플리케이션내의
		//자원의 위치등을 반환해주기도 한다..
		ServletContext context=config.getServletContext(); //jsp에서의 application 내장객체!!
		String mappingPath = context.getRealPath("/WEB-INF/mapping.data"); //linux, window, unix 맞게..
		props = new Properties();
		try {
			fis = new FileInputStream(mappingPath);
			props.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doRequest(request, response);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doRequest(request, response);
	}

	protected void doRequest(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		System.out.println("나 호출?");
		request.setCharacterEncoding("utf-8");
		// 받은 요청을 분석해보자!!(영화?, 혈액형?, 핸드폰, 세탁기 ...)
		String uri = request.getRequestURI();
		System.out.println("당신이 요청한 URI는 " + uri);
		Class controllerClass = null;
		Controller controller = null;

		// 영화 전담 컨테이너에게 요청을 전달하자!!
		// controller = new MovieController();

		String className = (String) props.get(uri); // 검색

		try {
			controllerClass = Class.forName(className);
			controller = (Controller) controllerClass.newInstance();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		controller.execute(request, response);
		
		//클라이언트에게 보여질 결과 페이지 (뷰) 셋팅
		//포워딩 객체 생성
		 RequestDispatcher dis = null;
		 String viewPage=(String)props.get(controller.getViewName());
		 dis = request.getRequestDispatcher(viewPage);
		 System.out.println("검색된 결과페이지 : "+viewPage);
		 //setAttribute를 이용하면,String 포함된 모든 객체를 담을 수 잏다!

		 //생성된 포워딩 객체를 이용하면 됨
		 dis.forward(request, response);
	}

	// 서블릿의 인스턴스가 소멸할때 호출되는 메서드!!
	@Override
	public void destroy() {
		if (fis != null) {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}