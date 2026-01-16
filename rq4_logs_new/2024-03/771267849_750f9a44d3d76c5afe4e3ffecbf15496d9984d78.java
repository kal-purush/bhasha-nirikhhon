package MiniProject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

//싱글톤 생성자가 private
//db연결해서 사용할 connection 객체를 반환
public class DBConnect {
	private static DBConnect dbconn = new DBConnect();
	private String url = "jdbc:oracle:thin:@192.168.0.19:1521/xe";
	
	private DBConnect() {}
	
	public Connection conn() {		
		try {
			//드라이버 로드. 클래스 이름으로 찾아서 메모리에 얹음
			Class.forName("oracle.jdbc.OracleDriver");
			//db에 로그인. 세션 수립하고 생성된 connection 객체 반환
			return DriverManager.getConnection(url, "hr", "hr");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static DBConnect getInstance() {
		return dbconn;
	}
	
}