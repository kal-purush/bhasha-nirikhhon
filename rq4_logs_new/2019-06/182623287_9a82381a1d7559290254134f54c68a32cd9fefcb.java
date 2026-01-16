package movie.controller;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class MovieCreateServlet extends HttpServlet {

	//サニタイジング追加
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException{
		String movieName=req.getParameter("movieName");
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		Date releaseDate=null,finishDate=null;
		try {
			releaseDate=sdf.parse(req.getParameter("releaseStartDate"));
			if(req.getParameter("releaseFinishDate")!=null) {
				finishDate=sdf.parse(req.getParameter("releaseFinishDate"));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String directed=req.getParameter("direcred");
		String cast=req.getParameter("cast");
		int ticketPrice=Integer.parseInt(req.getParameter("feeType"));
		String detail=req.getParameter("movieDetail");
	}

}