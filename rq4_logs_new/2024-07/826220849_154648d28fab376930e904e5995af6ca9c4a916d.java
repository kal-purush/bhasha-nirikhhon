package com.coach_member.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CoachMemberDAO implements CoachMemberDAO_interface {
	String driver = "com.mysql.cj.jdbc.Driver";
	String url = "jdbc:mysql://localhost:3306/tia102g3?serverTimezone=Asia/Taipei";
	String userid = "root";
	String passwd = "123456";

	private static final String INSERT_STMT = 
		"INSERT INTO coachmember (personalPhotos, status) VALUES (?, ?)";
	private static final String GET_ALL_STMT = 
		"SELECT coachMemberID, personalPhotos, status FROM coachmember order by coachMemberID";
	private static final String GET_ONE_STMT = 
		"SELECT coachMemberID, personalPhotos, status FROM coachmember WHERE coachMemberID = ?";
	private static final String DELETE = 
		"DELETE FROM coachmember WHERE coachMemberID = ?";
	private static final String UPDATE = 
		"UPDATE coachmember SET  personalPhotos = ?, status = ? WHERE coachMemberID = ?";

	@Override
	public void insert(CoachMemberVO coachMemberVO) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(INSERT_STMT);

			pstmt.setBytes(1, coachMemberVO.getPersonalPhotos());
			pstmt.setString(2, coachMemberVO.getStatus());
			
			pstmt.executeUpdate();

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. "
					+ e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. "
					+ se.getMessage());
			// Clean up JDBC resources
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException se) {
					se.printStackTrace(System.err);
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
			}
		}

	}

	@Override
	public void update(CoachMemberVO coachMemberVO) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(UPDATE);

			pstmt.setBytes(1, coachMemberVO.getPersonalPhotos());
			pstmt.setString(2, coachMemberVO.getStatus());
			pstmt.setDouble(3, coachMemberVO.getCoachMemberID());

			pstmt.executeUpdate();

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. "
					+ e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. "
					+ se.getMessage());
			// Clean up JDBC resources
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException se) {
					se.printStackTrace(System.err);
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
			}
		}

	}

	@Override
	public void delete(Double coachMemberID) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(DELETE);

			pstmt.setDouble(1, coachMemberID);

			pstmt.executeUpdate();

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. "
					+ e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. "
					+ se.getMessage());
			// Clean up JDBC resources
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException se) {
					se.printStackTrace(System.err);
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
			}
		}

	}

	@Override
	public CoachMemberVO getOne(Double coachMemberID) {

		CoachMemberVO coachMemberVO = null;
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(GET_ONE_STMT);

			pstmt.setDouble(1, coachMemberID);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				// coachMemberVO 也稱為 Domain objects
				coachMemberVO = new CoachMemberVO();
				coachMemberVO.setCoachMemberID(rs.getDouble("coachMemberID"));
				coachMemberVO.setPersonalPhotos(rs.getBytes("personalPhotos"));
				coachMemberVO.setStatus(rs.getString("status"));
			}

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. "
					+ e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. "
					+ se.getMessage());
			// Clean up JDBC resources
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException se) {
					se.printStackTrace(System.err);
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException se) {
					se.printStackTrace(System.err);
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
			}
		}
		return coachMemberVO;
	}

	@Override
	public List<CoachMemberVO> getAll() {
		List<CoachMemberVO> list = new ArrayList<CoachMemberVO>();
		CoachMemberVO coachMemberVO = null;

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(GET_ALL_STMT);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				// coachMemberVO 也稱為 Domain objects
				coachMemberVO = new CoachMemberVO();
				coachMemberVO.setCoachMemberID(rs.getDouble("coachMemberID"));
				coachMemberVO.setPersonalPhotos(rs.getBytes("personalPhotos"));
				coachMemberVO.setStatus(rs.getString("status"));
				list.add(coachMemberVO); // Store the row in the list
			}

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. "
					+ e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. "
					+ se.getMessage());
			// Clean up JDBC resources
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException se) {
					se.printStackTrace(System.err);
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException se) {
					se.printStackTrace(System.err);
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
			}
		}
		return list;
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) {

		CoachMemberDAO dao = new CoachMemberDAO();

		// 新增
		CoachMemberVO coachMemberVO1 = new CoachMemberVO();
//		coachMemberVO1.setPersonalPhotos("");
		coachMemberVO1.setStatus("未通過");
//		
//		dao.insert(coachMemberVO1);

		// 修改
		CoachMemberVO coachMemberVO2 = new CoachMemberVO();
		coachMemberVO2.setCoachMemberID(1.0);
//		coachMemberVO2.setPersonalPhotos("");
		coachMemberVO2.setStatus("已通過");

		dao.update(coachMemberVO2);

		// 刪除
//		dao.delete(7014);

		// 查詢
		CoachMemberVO coachMemberVO3 = dao.getOne(1.0);
		System.out.print(coachMemberVO3.getPersonalPhotos() + ",");
		System.out.print(coachMemberVO3.getStatus() + ",");
		System.out.println("---------------------");
		
		// 查詢
//		List<CoachMemberVO> list = dao.getAll();
//		for (CoachMemberVO aCoachMemberVO : list) {
		System.out.print(coachMemberVO3.getPersonalPhotos() + ",");
		System.out.print(coachMemberVO3.getStatus() + ",");
			System.out.println();
//		}
	}
}