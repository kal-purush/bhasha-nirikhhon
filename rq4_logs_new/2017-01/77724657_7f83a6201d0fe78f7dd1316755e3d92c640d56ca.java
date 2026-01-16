package com.mingttong.fruitweb;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ServletSearchGoods extends HttpServlet {
	private static final long serialVersionUID = 1L;

    public ServletSearchGoods() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		// 每页显示的记录数目
		int numPerPage = 10;
		
		// 获取参数
		String keyword = request.getParameter("keyword");
		String sPage = request.getParameter("page"); // 当前页面？
		
		int iPage = 1;
		
		try {
			iPage = Integer.parseInt(sPage);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		
		// 获取符合条件总记录数
		GoodsDAO dao = new GoodsDAO();
		
		// 获取记录总数
		int totalResNum = dao.countByKeyword(keyword);
		
		// 计算总页面数，校验当前页码
		int totalPgNum = totalResNum / numPerPage;
		
		if (totalResNum % numPerPage > 0) {
			totalPgNum++; // 不足一页的部分另起一页
		}
		
		if (iPage < 1) {
			iPage = 1;
		}
		
		if (iPage > totalPgNum) {
			iPage = totalPgNum;
		}
		
		// 根据页码查询符合条件的记录
		List<GoodsVO> result = dao.searchByKeyword(keyword, iPage);
		
		// 页面跳转以及传值（确保执行页面跳转后doPost执行完毕）
		request
	}

}