package movie.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import movie.model.reserveModel;

@WebServlet("/MovieReservationController")
public class MovieReservationController extends HttpServlet {
	protected void doGet(HttpServletRequest request , HttpServletResponse response)
			throws ServletException , IOException{

		//予約を複数同時に行えるように、座席番号と料金種別を保持するリストを作成する
		List SheetNumber = new ArrayList<Integer>();
		List FeeType = new ArrayList<String>();


		int MovieTermNumber = (int)request.getAttribute("term");
		String TheaterId = (String)request.getAttribute("theater");
		int ScreenNumber = (int)request.getAttribute("screen");
		int MemberNumber = (int)request.getAttribute("member");

		//とりあえず、予約を一つだけ登録できるようにする
		//複数同時予約の場合は、splitか何か使ってリストに入れれるようにしたいかも
		SheetNumber.add((Integer)request.getAttribute("sheet"));
		FeeType.add((String)request.getAttribute("fee"));

		//映画予約するreserveModelを呼び出す
		reserveModel reserve = new reserveModel();

		reserve.reserveMovie(MovieTermNumber, TheaterId, ScreenNumber, MemberNumber, SheetNumber, FeeType);

		response.sendRedirect("MovieReservationFinishController");
	}
}