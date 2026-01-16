package chatServer;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Calendar;
import java.util.List;
import java.util.Vector;

// 뷰 실행 후 사용자 접속 받는 쓰레드
public class SocketThread extends Thread {
	ChatDao 		 		 dao 			= 		null; // DB전담하여 쿼리문 질의하는 객체
	TalkServerThread 		 tst 			= 		null; // 각 클라이언트의 통신담당하는 쓰레드1
	List<TalkServerThread> globalList 		= 		null; // 각 클라이언트의 정보를 받음 (vector로 구현)
	ServerSocket 			server 			= 		null; // ip와 port 바인드하여 클라이언트 접속을 받는 객체
	Socket 					socket 			= 		null; // 클라이언트와 연결 되면 얻어지는 객체
	TalkServerView 			 view			= 		null;
	
	public SocketThread(TalkServerView view){
		this.view = view;	
	}
	
	@Override
	public void run() {
		// 서버에 접속해온 클라이언트 쓰레드 정보를 관리할 벡터 생성하기
		globalList = 	new Vector<>();
		dao 	   = 	new ChatDao(); // DB전담
		try {	
			boolean isStop 	= 	false;
			server 			= 	new ServerSocket(3002);
			view.jta_log.append("사용자의 접속을 기다리는 중입니다:)\n");
			while (!isStop) {
				view.jtf_userCount.setText("현재 접속인원은 " + globalList.size() + "명 입니다.");
				socket = server.accept();
				view.jta_log.append("client info:" + socket + "\n"); // 사용자 정보를 찍음 (ip, port)등
				TalkServerThread tst = new TalkServerThread(this); // 톡서버쓰레드 생성자에 자기자신이 들어간다.
				tst.start(); // 톡서버 쓰레드 시작
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// 해당 날짜 출력 메소드
	public String getDate() { // 변경
		Calendar today = Calendar.getInstance();
		int yyyy = today.get(Calendar.YEAR);
		int mm = today.get(Calendar.MONTH) + 1;
		int day = today.get(Calendar.DAY_OF_MONTH);
		return yyyy + "-" + (mm < 10 ? "0" + mm : "" + mm) + "-" + (day < 10 ? "0" + day : " " + day + " ");

	}

	// 해당시간 출력 메소드
	public String getTime() {
		Calendar today = Calendar.getInstance();
		int h = today.get(Calendar.HOUR_OF_DAY);
		int m = today.get(Calendar.MINUTE);
		String todayTime = (h < 10 ? "0" + h + "시 " : "" + h + "시 ") + (m < 10 ? "0" + m + "분" : "" + m + "분");

		return todayTime;
	}
	
	
}