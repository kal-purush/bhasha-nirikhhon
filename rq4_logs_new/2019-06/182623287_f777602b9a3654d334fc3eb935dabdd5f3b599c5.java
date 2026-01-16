package movie.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import movie.beans.MovieListBeans;
import movie.model.MovieModel;

@WebServlet("/admimtop")
public class AdminMovieListController extends HttpServlet {
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
												throws ServletException, IOException {
		//////////////////////////////
		//映画の一覧リストを取得
		List<MovieListBeans> list=new ArrayList<MovieListBeans>();
        MovieModel movieModel=new MovieModel();


        list=movieModel.getList();

		/////////////////////////////
		//映画の一覧をリクエストにセット
		request.setAttribute("list", list);

		RequestDispatcher dispatcher = request.getRequestDispatcher("WEB-INF/jsp/adminMovieList.jsp");
		dispatcher.forward(request, response);
	}


}