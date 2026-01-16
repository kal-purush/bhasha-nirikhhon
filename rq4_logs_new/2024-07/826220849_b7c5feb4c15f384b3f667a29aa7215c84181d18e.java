package com.tia102g3.food.controller;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tia102g3.food.model.FoodServiceImpl;
import com.tia102g3.food.model.FoodVO;

//import com.emp.model.*;
@WebServlet("/back-end/food/food.do")
public class FoodServlet extends HttpServlet {

	public void doGet(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {
		doPost(req, res);
	}

	public void doPost(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {

		req.setCharacterEncoding("UTF-8");
		String action = req.getParameter("action");
		
		
		if ("getOne_For_Display".equals(action)) { // �Ӧ�select_page.jsp���ШD

			List<String> errorMsgs = new LinkedList<String>();
			// Store this set in the request scope, in case we need to
			// send the ErrorPage view.
			req.setAttribute("errorMsgs", errorMsgs);

			/***************************1.接收請求參數 - 輸入格式的錯誤處理**********************/
				String str = req.getParameter("foodNumber");
				if (str == null || (str.trim()).length() == 0) {
					errorMsgs.add("請輸入食材編號");
				}
				// Send the use back to the form, if there were errors
				if (!errorMsgs.isEmpty()) {
					RequestDispatcher failureView = req
							.getRequestDispatcher("/back-end/food/select_page.jsp");
					failureView.forward(req, res);
					return; // program break
				}
				
				Integer foodNumber = null;
				try {
					foodNumber = Integer.valueOf(str);
				} catch (Exception e) {
					errorMsgs.add("食材編號格式不正確");
				}
				// Send the use back to the form, if there were errors
				if (!errorMsgs.isEmpty()) {
					RequestDispatcher failureView = req
							.getRequestDispatcher("/back-end/food/select_page.jsp");
					failureView.forward(req, res);
					return; // program break
				}
				
				/***************************2.開始查詢資料*****************************************/
				FoodServiceImpl foodSvc = new FoodServiceImpl();
				FoodVO foodVO = foodSvc.getOneFood(foodNumber);
				if (foodVO == null) {
					errorMsgs.add("查無資料");
				}
				// Send the use back to the form, if there were errors
				if (!errorMsgs.isEmpty()) {
					RequestDispatcher failureView = req
							.getRequestDispatcher("/back-end/food/select_page.jsp");
					failureView.forward(req, res);
					return; // program break
				}
				
				/***************************3.查詢完成,準備轉交(Send the Success view)*************/
				req.setAttribute("foodVO", foodVO); // 資料庫取出的FoodVO物件,存入req
				String url = "/back-end/food/listOneFood.jsp";
				RequestDispatcher successView = req.getRequestDispatcher(url); // 成功轉交 listOneFood.jsp
				successView.forward(req, res);
		}
		
		
		if ("getOne_For_Update".equals(action)) { // 來自listAllFood.jsp的請求

			List<String> errorMsgs = new LinkedList<String>();
			// Store this set in the request scope, in case we need to
			// send the ErrorPage view.
			req.setAttribute("errorMsgs", errorMsgs);
			
				/***************************1.接收請求參數 - 輸入格式的錯誤處理**********************/
				Integer foodNumber = Integer.valueOf(req.getParameter("foodNumber"));
				
				/***************************2.開始查詢資料*****************************************/
				FoodServiceImpl foodSvc = new FoodServiceImpl();
				FoodVO foodVO = foodSvc.getOneFood(foodNumber);
								
				/***************************3.查詢完成,準備轉交(Send the Success view)*************/
				req.setAttribute("foodVO", foodVO);         // 資料庫取出的FoodVO物件,存入req
				String url = "/back-end/food/update_food_input.jsp";
				RequestDispatcher successView = req.getRequestDispatcher(url);// ���\��� update_emp_input.jsp
				successView.forward(req, res);
		}
		
		
		if ("update".equals(action)) { // �Ӧ�update_emp_input.jsp���ШD
			
			List<String> errorMsgs = new LinkedList<String>();
			// Store this set in the request scope, in case we need to
			// send the ErrorPage view.
			req.setAttribute("errorMsgs", errorMsgs);
		
				/************** 1. format check **************/
				Integer foodNumber = Integer.valueOf(req.getParameter("foodNumber").trim());
				Integer foodTypeNumber = null;
				try {
					foodTypeNumber = Integer.valueOf(req.getParameter("foodTypeNumber").trim());
				}catch(NumberFormatException e) {
					foodTypeNumber = 0;
					errorMsgs.add("食材編號格式錯誤");
				}
				
				String foodName = req.getParameter("foodName").trim();
				if(foodName == null || foodName.trim().length() == 0) {
					errorMsgs.add("請輸入食材名稱");
				}
				
				Integer foodCalories = null;
				try {
					foodCalories = Integer.valueOf(req.getParameter("foodCalories").trim());
				}catch(NumberFormatException e) {
					foodCalories = 0;
					errorMsgs.add("熱量格式錯誤");
				}

				FoodVO foodVO = new FoodVO();
				foodVO.setFoodNumber(foodNumber);
				foodVO.setFoodTypeNumber(foodTypeNumber);
				foodVO.setFoodName(foodName);
				foodVO.setFoodCalories(foodCalories);

				// Send the use back to the form, if there were errors
				if (!errorMsgs.isEmpty()) {
req.setAttribute("foodVO", foodVO); // �t����J�榡���~��empVO����,�]�s�Jreq
					RequestDispatcher failureView = req
							.getRequestDispatcher("/back-end/food/update_food_input.jsp");
					failureView.forward(req, res);
					return; //�{�����_
				}
				
				/***************************2.�}�l�ק���*****************************************/
				FoodServiceImpl foodSvc = new FoodServiceImpl();
				foodVO = foodSvc.updateFood(foodNumber, foodTypeNumber, foodName, foodCalories);
				
				/***************************3.�ק粒��,�ǳ����(Send the Success view)*************/
				req.setAttribute("foodVO", foodVO); // ��Ʈwupdate���\��,���T����empVO����,�s�Jreq
				String url = "/back-end/food/listOneFood.jsp";
				RequestDispatcher successView = req.getRequestDispatcher(url); // �ק令�\��,���listOneEmp.jsp
				successView.forward(req, res);
		}

        if ("insert".equals(action)) { // �Ӧ�addEmp.jsp���ШD  
			
			List<String> errorMsgs = new LinkedList<String>();
			// Store this set in the request scope, in case we need to
			// send the ErrorPage view.
			req.setAttribute("errorMsgs", errorMsgs);

				/************** 1. format check **************/
				Integer foodNumber = null;
				
				Integer foodTypeNumber = null;
				try {
					foodTypeNumber = Integer.valueOf(req.getParameter("foodTypeNumber").trim());
				}catch(NumberFormatException e) {
					foodTypeNumber = 0;
					errorMsgs.add("食材編號格式錯誤");
				}
				
				String foodName = req.getParameter("foodName").trim();
				if(foodName == null || foodName.trim().length() == 0) {
					errorMsgs.add("請輸入食材名稱");
				}
				
				Integer foodCalories = null;
				try {
					foodCalories = Integer.valueOf(req.getParameter("foodCalories").trim());
				}catch(NumberFormatException e) {
					foodCalories = 0;
					errorMsgs.add("熱量格式錯誤");
				}
				
				FoodVO foodVO = new FoodVO();
				foodVO.setFoodNumber(foodNumber);
				foodVO.setFoodTypeNumber(foodTypeNumber);
				foodVO.setFoodName(foodName);
				foodVO.setFoodCalories(foodCalories);

				// Send the use back to the form, if there were errors
				if (!errorMsgs.isEmpty()) {
req.setAttribute("foodVO", foodVO); // �t����J�榡���~��empVO����,�]�s�Jreq
					RequestDispatcher failureView = req
							.getRequestDispatcher("/back-end/food/addFood.jsp");
					failureView.forward(req, res);
					return;
				}
				
				/***************************2.�}�l�s�W���***************************************/
				FoodServiceImpl foodSvc = new FoodServiceImpl();
				foodVO = foodSvc.addFood(foodNumber, foodTypeNumber, foodName, foodCalories);
				
				/***************************3.�s�W����,�ǳ����(Send the Success view)***********/
				String url = "/back-end/food/listAllFood.jsp";
				RequestDispatcher successView = req.getRequestDispatcher(url); // �s�W���\�����listAllEmp.jsp
				successView.forward(req, res);				
		}
		
		
		if ("delete".equals(action)) { // �Ӧ�listAllEmp.jsp

			List<String> errorMsgs = new LinkedList<String>();
			// Store this set in the request scope, in case we need to
			// send the ErrorPage view.
			req.setAttribute("errorMsgs", errorMsgs);
	
				/***************************1.�����ШD�Ѽ�***************************************/
				Integer foodNumber = Integer.valueOf(req.getParameter("foodNumber"));
				
				/***************************2.�}�l�R�����***************************************/
				FoodServiceImpl foodSvc = new FoodServiceImpl();
				foodSvc.deleteFood(foodNumber);
				
				/***************************3.�R������,�ǳ����(Send the Success view)***********/								
				String url = "/back-end/food/listAllFood.jsp";
				RequestDispatcher successView = req.getRequestDispatcher(url);// �R�����\��,���^�e�X�R�����ӷ�����
				successView.forward(req, res);
		}
		
		// Image test
		if("getImage".equals(action)) {
			
			req.setAttribute("imageNumber", req.getParameter("imageNumber"));
			req.setAttribute("myName", 321);
//			System.out.println(req.getParameter("imageNumber"));
			
			String url = "/back-end/food/showImage.jsp";
			RequestDispatcher successView = req.getRequestDispatcher(url);// �R�����\��,���^�e�X�R�����ӷ�����
			successView.forward(req, res);
			
		}
	}
}