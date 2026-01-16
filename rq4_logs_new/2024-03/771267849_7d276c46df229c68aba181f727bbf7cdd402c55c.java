package MiniProject.Record;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import MiniProject.DBConnect;

public class RecordDao {
	private DBConnect db;

	public RecordDao() {
		db = DBConnect.getInstance();
	}

	// 거래 내역 추가
	public void insert(Record r) {
		Connection conn = db.conn();
		String sql = "insert into Record values(seq.nextvalue,?,?,?,?,sysdate,?)";
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, r.getAccount_num());
			pstmt.setInt(2, r.getMoney());
			pstmt.setString(3, r.getName());
			pstmt.setInt(4, r.getBalnace());
			pstmt.setInt(5, r.getIsDeposit());
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}


//날짜별로 조회
	public ArrayList<Record> selectByDate(String newDate) {
		Connection conn = db.conn();
		String sql = "select * from Record where to_char(newDate, 'yy/mm/dd') = ?";
		ArrayList<Record> list = new ArrayList<Record>();
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, newDate);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				list.add(new Record(rs.getInt(1),rs.getInt(2),rs.getInt(3),rs.getString(4),rs.getInt(5),rs.getDate(6),rs.getInt(7)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return list;
	}

//입금만 조회
	public ArrayList<Record> selectByDeposit(int type) {//입금:1, 출금:2
		Connection conn = db.conn();
		String sql = "select * from record where isDeposit = 1"; //입금
		ArrayList<Record> list = new ArrayList<Record>();
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, type);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				list.add(new Record(rs.getInt(1),rs.getInt(2),rs.getInt(3),rs.getString(4),rs.getInt(5),rs.getDate(6),rs.getInt(7)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO: handle exception
			}
	} 
		return list;
	}

//출금만 조회
	public ArrayList<Record> selectByWithdraw(int type) {
		Connection conn = db.conn();
		String sql2 = "select * from record where isDeposit=0"; //출금
		ArrayList<Record> list = new ArrayList<Record>();
	try {
		PreparedStatement pstmt = conn.prepareStatement(sql2);
		pstmt.setInt(1, type);
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			list.add(new Record(rs.getInt(1),rs.getInt(2),rs.getInt(3),rs.getString(4),rs.getInt(5),rs.getDate(6),rs.getInt(7)));
		}
	} catch (SQLException e) {
		e.printStackTrace();
	} finally {
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO: handle exception
		}
} 
	return list;
}

// 거래 내역 조회	
	public ArrayList<Record> selectAll() {
		Connection conn = db.conn();
		String sql = "select * from record order by num";
		ArrayList<Record> list = new ArrayList<Record>();
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				list.add(new Record(rs.getInt(1),rs.getInt(2),rs.getInt(3),rs.getString(4),rs.getInt(5),rs.getDate(6),rs.getInt(7)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
}

	

	
