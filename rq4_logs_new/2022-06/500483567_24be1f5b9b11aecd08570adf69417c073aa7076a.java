package chatClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import chatServer.ChatMsgVO;
import chatServer.DButil;

public class LoginDao {

	Connection con;
	PreparedStatement pstmt;
	ResultSet rs;
	List<ChatMsgVO> list;

	// 회원가입 메소드(signUp) -- 회원가입 버튼에서 호출
	public void signUp(String _id, String _pw, String _name, String _phone) {
		String sql = "INTSERT INTO MEMBER VALUES(?,?,?)";

		con = DButil.getOracleConnection(); // DButil에서 예외처리 했으므로 따로 해주지 않아도 됨
		try {
			pstmt = con.prepareStatement(sql);
			pstmt.setString(1, _id);
			pstmt.setString(2, _pw);
			pstmt.setString(3, _name);
			pstmt.setString(4, _phone);
			int i = pstmt.executeUpdate();
			System.out.println("데이터 " + i + "건이 추가되었습니다");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		DButil.close(con, pstmt);

	}

	// 아이디 중복검사 메소드(idCheck) -- 중복검사 버튼에서 호출
	public int idCheck(String _id) {
		String sql = "SELECT ID FROM MEMBER WHERE ID = ?"; // id가 있다면
		int result = 0;

		con = DButil.getOracleConnection();
		try {
			pstmt = con.prepareStatement(sql);
			pstmt.setString(1, _id);
			rs = pstmt.executeQuery();
			if (rs.next()) { // rs.next()가 true라는 것은 id가 있다는 것
				result = 1; // 아이디 중복이 없다면 1반환
			} else
				result = -1; // 중복이 있다면 -1반환
		} catch (SQLException e) {
			e.printStackTrace();
		}
		DButil.close(con, pstmt, rs); // 닫기

		return result;
	}

	// 로그인 확인 메소드 -- 로그인 버튼에서 호출
	// 로그인 성공시 1반환, 비밀번호 불일치 0반환, 아이디 없을 경우 -1반환
	public int login(String _id, String _pw) {
		String sql = "SELECT * FROM MEMBER WHERE ID = ?";
		int result = 0;
		con = DButil.getOracleConnection();
		try {
			pstmt = con.prepareStatement(sql);
			pstmt.setString(1, _id);
			rs = pstmt.executeQuery();

			if (rs.next()) { // 아이디가 일치하고 비밀번호가 일치할 경우 1, 비밀번호 틀렸을 경우 0 반환, 아이디 없을경우 -1
					if (rs.getString("pw").equals(_pw)) { // String의 오버라이딩 된 equals로 비밀번호 확인
						System.out.println("로그인 합니다"); // 단위테스트용
						result= 1;
					} else if(rs.getString("PW") != _pw) {
						System.out.println("비밀번호가 틀렸습니다"); // 단위테스트용
						result = 0;
					}
				}
				else {
					System.out.println("입력하신 아이디는 존재하지 않습니다"); // 단위테스트용
					result = -1;
				}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		DButil.close(con, pstmt, rs);
		return result;
	}

	// 로그인 후 닉네임 물어오기
	// view에서 if처리(login()메소드 리턴값 1이면 name()메소드 실행 되도록 작성)
	public String name(String _id) {
		String sql = "SELECT NICNAME FROM MEMBER WHERE ID = ?";
		String nicname = "";
		con = DButil.getOracleConnection();
		try {
			pstmt = con.prepareStatement(sql);
			pstmt.setString(1, _id);
			rs = pstmt.executeQuery();

			if (rs.next()) {
				nicname = rs.getString("NICNAME");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		DButil.close(con, pstmt, rs);
		return nicname;
	}

}