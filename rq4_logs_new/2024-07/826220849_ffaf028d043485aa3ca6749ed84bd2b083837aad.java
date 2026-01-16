package com.tia102g3.admin.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.tia102g3.order.model.OrderVO;

public class AdminJDBCDAO implements AdminDAOInterface {
	String driver = "com.mysql.cj.jdbc.Driver";
	String url = "jdbc:mysql://localhost:3306/tia102g3?serverTimezone=Asia/Taipei";
	String userid = "root";
	String passwd = "123456";

	private static final String INSERT_STMT = "INSERT INTO adminid (adminName, adminUserName, adminPassword, adminEmail) VALUES (?, ?, ?, ?)";
	private static final String GET_ALL_STMT = "SELECT adminID, adminName, adminUserName, adminPassword, adminEmail FROM adminid ORDER BY adminID";
	private static final String GET_ONE_STMT = "SELECT adminID, adminName, adminUserName, adminPassword, adminEmail FROM adminid WHERE adminID = ?";
	private static final String DELETE = "DELETE FROM adminid WHERE adminID = ?";
	private static final String UPDATE = "UPDATE adminid SET adminName = ?, adminUserName = ?, adminPassword = ?, adminEmail = ? WHERE adminID = ?";

	@Override
	public void insert(AdminVO adminVO) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(INSERT_STMT);

			pstmt.setString(1, adminVO.getAdminName());
			pstmt.setString(2, adminVO.getAdminUserName());
			pstmt.setString(3, adminVO.getAdminPassword());
			pstmt.setString(4, adminVO.getAdminEmail());

			pstmt.executeUpdate();

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. " + e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. " + se.getMessage());
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
	public void update(AdminVO adminVO) {
		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(UPDATE);
			
			pstmt.setInt(1, adminVO.getAdminID());
			pstmt.setString(2, adminVO.getAdminName());
			pstmt.setString(3, adminVO.getAdminUserName());
			pstmt.setString(4, adminVO.getAdminPassword());
			pstmt.setString(5, adminVO.getAdminEmail());
			

			pstmt.executeUpdate();

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. " + e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. " + se.getMessage());
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
	public void delete(Integer adminID) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(DELETE);

			pstmt.setInt(1, adminID);

			pstmt.executeUpdate();

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. " + e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. " + se.getMessage());
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
	public AdminVO findByPrimaryKey(Integer adminID) {

		AdminVO adminVO = null;
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(GET_ONE_STMT);

			pstmt.setInt(1, adminID);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				// empVo 也稱為 Domain objects
				adminVO = new AdminVO();
				adminVO.setAdminID(rs.getInt("adminID"));
				adminVO.setAdminName(rs.getString("adminName"));
				adminVO.setAdminUserName(rs.getString("adminUserName"));
				adminVO.setAdminPassword(rs.getString("adminPassword"));
				adminVO.setAdminEmail(rs.getString("adminEmail"));
			}

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. " + e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. " + se.getMessage());
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
		return adminVO;
	}
	
	

	@Override
	public List<AdminVO> getAll() {
		List<AdminVO> list = new ArrayList<AdminVO>();
		AdminVO adminVO = null;

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(GET_ALL_STMT);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				// empVO 也稱為 Domain objects
				adminVO = new AdminVO();
				adminVO.setAdminID(rs.getInt("adminID"));
				adminVO.setAdminName(rs.getString("adminName"));
				adminVO.setAdminUserName(rs.getString("adminUserName"));
				adminVO.setAdminPassword(rs.getString("adminPassword"));
				adminVO.setAdminEmail(rs.getString("adminEmail"));
				list.add(adminVO); // Store the row in the list
			}

			// Handle any driver errors
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load database driver. " + e.getMessage());
			// Handle any SQL errors
		} catch (SQLException se) {
			throw new RuntimeException("A database error occured. " + se.getMessage());
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

}

//	public static void main(String[] args) {
//
//		OrderJDBCDAO dao = new OrderJDBCDAO();
//
//		// 新增
//		AdminVO adminVO1 = new AdminVO();
//		adminVO1.setAdminName("David");
//		adminVO1.setAdminUserName("102david");
//		adminVO1.setAdminPassword("a122345");
//		adminVO1.setAdminEmail("xxx@gmail.com");
//		dao.insert(adminVO1);
//
//		// 修改
//		AdminVO adminVO2 = new AdminVO();

//		adminVO2.setAdminID(2);
//		adminVO2.setAdminName("Finn");
//		adminVO2.setAdminUserName("102finn");
//		adminVO2.setAdminPassword("b122345");
//		adminVO2.setAdminEmail("aaa@gmail.com");
//		dao.update(adminVO2);
//
//		// 刪除
//		dao.delete(3);
//
////		// 查詢
//		OAdminVO adminVO3 = dao.findByPrimaryKey(4);
//		System.out.print(adminVO3.getAdminID() + ",");
//		System.out.print(orderVO3.getAdminName() + ",");
//		System.out.print(orderVO3.getAdminUserName() + ",");
//		System.out.print(orderVO3.getAdminPassword() + ",");
//		System.out.print(orderVO3.getAdminEmail() + ",");
//		System.out.println("---------------------");
//
//		// 查詢
//		List<AdminVO> list = dao.getAll();
//		for (AdminVO aOrder : list) {
//			System.out.print(aOrder.getAdminID() + ",");
//			System.out.print(aOrder.getAdminName() + ",");
//			System.out.print(aOrder.getAdminUserName() + ",");
//			System.out.print(aOrder.getAdminPassword() + ",");
//			System.out.print(aOrder.getAdminEmail() + ",");
//			System.out.println();
//		}
//	}
//
//	
//
//}