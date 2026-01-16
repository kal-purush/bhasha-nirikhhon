package examples.jdbcExam;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/*
 * MySql 연결 테스트.
 */
public class JDBCExam {
	public static void main(String[] args) {
		// JDBC프로그래밍 순서
		
		// 2. 커넥션을 연결한다. (DB와 접속한다)
		// jdbc url은 DBMS 마다 형식이 다르다.
		// 커넥션은 반드시 finally에서 close한다.
		Connection conn = null;
		
		// 3. SQL을 실행하기 위한 interface를 선언한다.
		PreparedStatement ps = null;
		
		// 4. 결과를 가져오기 위한 interface 를 선언한다.
		ResultSet rs = null;
		
		try {
			// 1. Driver 를 등록한다. try 안에 말고 밖에 위치시키려면
			// throws Exception 해주면 된다.
			Class.forName("com.mysql.jdbc.Driver");
			
			// 5. SQL 을 준비한다.
			String sql = "SELECT role_id, description FROM ROLE";
            String sql2 = "INSERT INTO ROLE (role_id, description) VALUES (? , ?)";
            
            // a. DB에 연결한다.
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/fastcampus","root","1234");
            
            // db 에 sql을 준비해달라고 하자.
            // ps 는 db안에서 준비된 sql을 참조하는 변수.
            ps = conn.prepareStatement(sql);
            
            // 준비된 sql을 실행하게 한다. 실행한 결과는 DBMS에 있다.
            // rs는 실행된 결과를 참조하는 변수.
            // executeQuery() : select 문 실행. executeUpdate() : insert,update,delect
            rs = ps.executeQuery();
            
            // 한건의 data를 가져온다. 한건의 data는 여러개의 컬럼으로 구성된다.
            // next()가 호출되면 한건을 읽어오는데 , 그 결과는 ResultSet 이 가지고 있다.
            while(rs.next()) {
            		int roleId = rs.getInt(1);
            		String description = rs.getString(2);
            		System.out.println(roleId + " , " + description);
            }
            
            // insert 문 실행
            ps = conn.prepareStatement(sql2);
            ps.setInt(1, 104);
            ps.setString(2, "Intern");
            ps.executeUpdate();
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			}catch(Exception e) {}
			
			try {
				ps.close();
			}catch(Exception e) {}
			
			try {
				conn.close();
			}catch(Exception e) {}
		}
		

	}

}