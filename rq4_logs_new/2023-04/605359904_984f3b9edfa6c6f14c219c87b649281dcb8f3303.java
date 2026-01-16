package DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import VO.ChatVO;

public class ChatDAO{
	
	Connection con; //데이터베이스 연결 객체
	DataSource ds; //커넥션 풀 객체

	public ChatDAO() {
		try {
		
			// 웹 프로젝트의 디렉터리에 접근하기 위한 객체 생성
			Context ctx = new InitialContext();
			// 커넥션 풀 얻기 
			ds = (DataSource)ctx.lookup("java:/comp/env/jdbc/oracle");
			// lookup 메소드로 "java:/comp/env/jdbc/oracle" 이름의 DataSource 객체를 찾아서 ds에 대입
			con = ds.getConnection();
			 // getConnection 메소드로 Connection 객체를 얻어서 con에 대입
			
		} catch (Exception e) {  // 예외 처리
			e.printStackTrace(); // 에러 메세지 출력
	
		}
		
	} // ChatDAO 생성자 종료
	
	// 채팅방 내 모든 채팅 내역을 가져오는 메서드
	public ArrayList<ChatVO> getChatList(String nowTime) {
		
		// 채팅 리스트를 담을 ArrayList 생성
		ArrayList<ChatVO> chatList = null;
		
		// PreparedStatement, ResultSet 객체 생성
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		// SQL문 생성
		String sql = "SELECT * FROM CHAT WHERE chatTime > ? ORDER BY chatTime";
		try {
			// PreparedStatement 객체 생성 및 파라미터 설정
			pstmt = con.prepareStatement(sql);
			pstmt.setString(1, nowTime);
			// SQL문 실행 후 ResultSet 객체에 결과 저장
			rs = pstmt.executeQuery();
			// chatList ArrayList 생성
			chatList = new ArrayList<ChatVO>();
			// ResultSet 객체를 순회하며 ChatVO 객체 생성 후 chatList에 추가
			while (rs.next()) {
				
				ChatVO chatvo = new ChatVO();// ChatVO 객체 생성
				chatvo.setChatID(rs.getInt("chatID"));// ChatVO 객체에 결과값 저장
				chatvo.setM_nickname(rs.getString("m_nickname"));
				
				// 채팅 내용에서 특정 문자열 치환하여 ChatVO에 저장
				chatvo.setChatContent(rs.getString("chatContent").replaceAll("","&nbsp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll("\n","<br>"));
				int chatTime = Integer.parseInt(rs.getString("chatTime").substring(11,13));
				String timeType = "오전";// 시간 타입 변수 초기화
				
				// 채팅 시간의 형식 변경
				if(Integer.parseInt(rs.getString("chatTime").substring(11,13))>= 12) {
					timeType = "오후";
					chatTime -= 12; // 오후 12시 이후의 경우에는 12를 뺀 시간으로 변경
				}
				chatvo.setChatTime(rs.getString("chatTime").substring(0,11) + " " + timeType + " " + chatTime + ":" + rs.getString("chatTime").substring(14,16)+ "" );
				chatList.add(chatvo);
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			// 자원 해제
			try {
				if(rs != null) rs.close();
				if(pstmt != null)pstmt.close();
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	
		// chatList 반환
		return chatList;
	}
	

	public ArrayList<ChatVO> getChatListByRecent(int number) {
		
		ArrayList<ChatVO> chatList = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "SELECT * FROM CHAT WHERE chatID > (SELECT MAX(chatID) - ? FROM CHAT) ORDER BY chatTime";
		
		try {
			
			pstmt = con.prepareStatement(sql); // SQL 쿼리문 실행을 위한 PreparedStatement 객체 생성
			pstmt.setInt(1, number); // 입력받은 number 값으로 파라미터 설정
			rs = pstmt.executeQuery(); // 쿼리 실행
			chatList = new ArrayList<ChatVO>(); // 채팅 리스트를 저장할 ArrayList 객체 생성
			while (rs.next()) { // 쿼리 결과가 있는 동안 반복문 실행
				ChatVO chatvo = new ChatVO();// ChatVO 객체 생성
				chatvo.setChatID(rs.getInt("chatID"));  // ChatVO 객체에 결과값 저장
				chatvo.setM_nickname(rs.getString("m_nickname"));
				chatvo.setChatContent(rs.getString("chatContent").replaceAll("","&nbsp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll("\n","<br>"));
				int chatTime = Integer.parseInt(rs.getString("chatTime").substring(11,13));
				String timeType = "오전"; // 시간 타입 변수 초기화
				if(Integer.parseInt(rs.getString("chatTime").substring(11,13))>= 12) {
					timeType = "오후";
					chatTime -= 12; // 오후 12시 이후의 경우에는 12를 뺀 시간으로 변경
				}
				chatvo.setChatTime(rs.getString("chatTime").substring(0,11) + " " + timeType + " " + chatTime + ":" + rs.getString("chatTime").substring(14,16)+ "" );
				chatList.add(chatvo); // ChatVO 객체를 ArrayList에 추가
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(rs != null) rs.close(); // 결과셋이 null이 아닐 경우 close() 메서드 호출
				if(pstmt != null)pstmt.close(); // PreparedStatement 객체가 null이 아닐 경우 close() 메서드 호출
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return chatList;  // 생성된 ArrayList 객체 반환
	}
	
	
	public ArrayList<ChatVO> getChatListByRecent(String chatID) {
	
		ArrayList<ChatVO> chatList = null;// 반환할 채팅 데이터 리스트 초기화
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		 // 데이터베이스에서 채팅 내용을 가져오는 SQL 쿼리
		String sql = "SELECT * FROM CHAT WHERE chatID > ? ORDER BY chatTime";
		
		try {
			pstmt = con.prepareStatement(sql);
			 // 가장 최근 채팅ID 이후의 채팅 내용을 가져오기 위해 매개변수로 전달받은 chatID를 사용하여 PreparedStatement의 매개변수를 설정
			pstmt.setInt(1, Integer.parseInt(chatID)); 
			
			rs = pstmt.executeQuery();// SQL 쿼리 실행
			chatList = new ArrayList<ChatVO>();// 채팅 데이터 리스트 초기화
			
			while (rs.next()) {
				ChatVO chatvo = new ChatVO(); // 가져온 채팅 데이터를 저장할 ChatVO 객체 생성
				chatvo.setChatID(rs.getInt("chatID"));  // 채팅 ID 설정
				chatvo.setM_nickname(rs.getString("m_nickname"));// 채팅을 입력한 사용자의 닉네임 설정
	            // 채팅 내용을 HTML 태그로 변환하여 ChatVO 객체의 채팅 내용 필드에 저장
				
				chatvo.setChatContent(rs.getString("chatContent").replaceAll("","&nbsp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll("\n","<br>"));
				int chatTime = Integer.parseInt(rs.getString("chatTime").substring(11,13)); // 채팅 입력 시간의 시 설정
				String timeType = "오전"; // 채팅 입력 시간의 오전/오후 설정
				if(Integer.parseInt(    rs.getString("chatTime").substring(11,13)     )>= 12) { // 만약 채팅 입력 시간이 오후 12시 이후라면
					timeType = "오후"; // timeType을 "오후"로 설정
					chatTime -= 12; // chatTime에서 12를 빼서 12시간제로 변환
				}
				 // ChatVO 객체의 채팅 입력 시간 필드에 저장
				chatvo.setChatTime(rs.getString("chatTime").substring(0,11) + " " + timeType + " " + chatTime + ":" + rs.getString("chatTime").substring(14,16)+ "" );
				chatList.add(chatvo);// 생성된 ChatVO 객체를 리스트에 추가
				
			}
		} catch (Exception e) {
			e.printStackTrace(); // 예외가 발생하면 콘솔에 출력
		}finally {
			try {
				if(rs != null) rs.close(); // ResultSet 종료
				if(pstmt != null)pstmt.close();// ResultSet 종료
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return chatList;
	}
	
	
	
	
	public int submit(String m_nickname, String chatContent) {
	
		PreparedStatement pstmt = null;
		ResultSet rs = null;
	
		// chatID는 시퀀스를 이용하여 자동으로 증가하도록 함
		// chatTime은 SYSDATE를 이용하여 현재 시간을 저장하도록 함
		String sql = "INSERT INTO CHAT(chatID,m_nickname,chatContent,chatTime) "
					+ "VALUES (chat_chatID.NEXTVAL,? , ?,   TO_CHAR(SYSDATE, 'YYYY-MM-DD HH:MI:SS') )";
		
		try {
			pstmt = con.prepareStatement(sql);
			pstmt.setString(1, m_nickname);
			pstmt.setString(2, chatContent);
			return pstmt.executeUpdate();// 쿼리 실행 결과 반환
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(rs != null) rs.close();
				if(pstmt != null)pstmt.close();
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return -1;// 쿼리 실행 실패 시 -1 반환
	}
}