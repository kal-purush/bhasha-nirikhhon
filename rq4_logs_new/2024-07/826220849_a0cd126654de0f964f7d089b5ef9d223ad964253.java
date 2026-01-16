package com.tia102g3order.model;

import java.util.*;
import java.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class OrderJDBCDAO implements OrderDAOInterface {
	String driver = "com.mysql.cj.jdbc.Driver";
	String url = "jdbc:mysql://localhost:3306/g3?serverTimezone=Asia/Taipei";
	String userid = "root";
	String passwd = "123456";

	  private static final String INSERT_STMT = 
		        "INSERT INTO orderid (memberID, orderDate, status, totalPrice) VALUES (?, ?, ?, ?)";
		    private static final String GET_ALL_STMT = 
		        "SELECT orderID, memberID, orderDate, status, totalPrice FROM orderid ORDER BY orderID";
		    private static final String GET_ONE_STMT = 
		        "SELECT orderID, memberID, orderDate, status, totalPrice FROM orderid WHERE orderID = ?";
		    private static final String DELETE = 
		        "DELETE FROM orderid WHERE orderID = ?";
		    private static final String UPDATE = 
		        "UPDATE orderid SET memberID = ?, orderDate = ?, status = ?, totalPrice = ? WHERE orderID = ?";

	@Override
	public void insert(OrderVO orderVO) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(INSERT_STMT);

			pstmt.setInt(1, orderVO.getMemberID());
			pstmt.setTimestamp(2, orderVO.getOrderDate());
			pstmt.setString(3, orderVO.getStatus());
			pstmt.setInt(4, orderVO.getTotalPrice());

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
	public void update(OrderVO orderVO) {
		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(UPDATE);

			pstmt.setInt(1, orderVO.getMemberID());
			pstmt.setTimestamp(2, orderVO.getOrderDate());
			pstmt.setString(3, orderVO.getStatus());
			pstmt.setInt(4, orderVO.getTotalPrice());
			pstmt.setInt(5, orderVO.getOrderID());

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
	public void delete(Integer orderID) {

		Connection con = null;
	    PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(DELETE);

			pstmt.setInt(1, orderID);

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
	public OrderVO findByPrimaryKey(Integer orderID) {

		OrderVO orderVO = null;
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(GET_ONE_STMT);

			pstmt.setInt(1, orderID);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				// empVo 也稱為 Domain objects
				orderVO = new OrderVO();
				orderVO.setOrderID(rs.getInt("orderID"));
				orderVO.setMemberID(rs.getInt("MemberID"));
				orderVO.setOrderDate(rs.getTimestamp("orderDate"));
				orderVO.setStatus(rs.getString("status"));
				orderVO.setTotalPrice(rs.getInt("totalPrice"));
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
		return orderVO;
	}

	@Override
	public List<OrderVO> getAll() {
		List<OrderVO> list = new ArrayList<OrderVO>();
		OrderVO orderVO = null;

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
				orderVO = new OrderVO();
				orderVO.setOrderID(rs.getInt("orderID"));
				orderVO.setMemberID(rs.getInt("MemberID"));
				orderVO.setOrderDate(rs.getTimestamp("orderDate"));
				orderVO.setStatus(rs.getString("status"));
				orderVO.setTotalPrice(rs.getInt("totalPrice"));
				list.add(orderVO); // Store the row in the list
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

	

	

//	public static void main(String[] args) {
//
//		OrderJDBCDAO dao = new OrderJDBCDAO();
//
//		// 新增
//		OrderVO orderVO1 = new OrderVO();
//		orderVO1.setMemberID(00005);
//		orderVO1.setOrderDate(java.sql.Timestamp.valueOf("2024-07-07 13:42:12"));
//		orderVO1.setStatus("已出貨");
//		orderVO1.setTotalPrice(499);
//		dao.insert(orderVO1);
//
//		// 修改
//		OrderVO orderVO2 = new OrderVO();
//		orderVO2.setOrderID(1001);
//		orderVO2.setMemberID(00002);
//		orderVO2.setOrderDate(java.sql.Timestamp.valueOf("2024-07-01 13:42:12"));
//		orderVO2.setStatus("已出貨");
//		orderVO2.setTotalPrice(499);
//		dao.update(orderVO2);
//
//		// 刪除
//		dao.delete(1002);
//
//		// 查詢
//		OrderVO orderVO3 = dao.findByPrimaryKey(1001);
//		System.out.print(orderVO3.getOrderID() + ",");
//		System.out.print(orderVO3.getMemberID() + ",");
//		System.out.print(orderVO3.getOrderDate() + ",");
//		System.out.print(orderVO3.getStatus() + ",");
//		System.out.print(orderVO3.getTotalPrice() + ",");
//		System.out.println("---------------------");
//
//		// 查詢
//		List<OrderVO> list = dao.getAll();
//		for (OrderVO aOrder : list) {
//			System.out.print(aOrder.getOrderID() + ",");
//			System.out.print(aOrder.getMemberID() + ",");
//			System.out.print(aOrder.getOrderDate() + ",");
//			System.out.print(aOrder.getStatus() + ",");
//			System.out.print(aOrder.getTotalPrice() + ",");
//			System.out.println();
//		}
//	}
//
	

}