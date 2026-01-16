package com.tia102g3.product.model;

import java.util.*;
import java.sql.*;

public class ProductJDBCDAO implements ProductDAO_interface {
	String driver = "com.mysql.cj.jdbc.Driver";
	String url = "jdbc:mysql://localhost:3306/tia102g3yun?serverTimezone=Asia/Taipei";
	String userid = "root";
	String passwd = "123456";

	private static final String INSERT_STMT = 
		"INSERT INTO product (prodName, price, productQuantity, intro) VALUES (?, ?, ?, ?)";
	private static final String GET_ALL_STMT = 
		"SELECT productID, prodName, price, productQuantity, intro FROM product order by productID";
	private static final String GET_ONE_STMT = 
		"SELECT productID, prodName, price, productQuantity, intro FROM product where productID = ?";
	private static final String DELETE = 
		"DELETE FROM product where productID = ?";
	private static final String UPDATE = 
		"UPDATE product set prodName=?, price=?, productQuantity=?, intro=? where productID = ?";

	@Override
	public void insert(ProductVO productVO) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(INSERT_STMT);

			pstmt.setString(1, productVO.getProdName());
			pstmt.setInt(2, productVO.getPrice());
			pstmt.setInt(3, productVO.getProductQuantity());
			pstmt.setString(4, productVO.getIntro());


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
	public void update(ProductVO productVO) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(UPDATE);

			pstmt.setString(1, productVO.getProdName());
			pstmt.setInt(2, productVO.getPrice());
			pstmt.setInt(3, productVO.getProductQuantity());
			pstmt.setString(4, productVO.getIntro());
			pstmt.setInt(5, productVO.getProductID());

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
	public void delete(Integer productID) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(DELETE);

			pstmt.setInt(1, productID);

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
	public ProductVO findByPrimaryKey(Integer productID) {

		ProductVO productVO = null;
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(GET_ONE_STMT);

			pstmt.setInt(1, productID);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				// empVo 也稱為 Domain objects
				productVO = new ProductVO();
				productVO.setProductID(rs.getInt("productID"));
				productVO.setProdName(rs.getString("prodName"));
				productVO.setPrice(rs.getInt("price"));
				productVO.setProductQuantity(rs.getInt("productQuantity"));
				productVO.setIntro(rs.getString("intro"));
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
		return productVO;
	}

	@Override
	public List<ProductVO> getAll() {
		List<ProductVO> list = new ArrayList<ProductVO>();
		ProductVO productVO = null;

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
				productVO = new ProductVO();
				productVO.setProductID(rs.getInt("productID"));
				productVO.setProdName(rs.getString("prodName"));
				productVO.setPrice(rs.getInt("price"));
				productVO.setProductQuantity(rs.getInt("productQuantity"));
				productVO.setIntro(rs.getString("intro"));
				list.add(productVO); // Store the row in the list
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

	public static void main(String[] args) {

		ProductJDBCDAO dao = new ProductJDBCDAO();

		System.out.println("tttt");
//		// 新增
//		ProductVO productVO1 = new ProductVO();
//		productVO1.setProdName("好讚狗飼料");
//		productVO1.setPrice(200);
//		productVO1.setProductQuantity(1);
//		productVO1.setIntro("主打經濟實惠");
//		dao.insert(productVO1);
//
//		// 修改
//		ProductVO productVO2 = new ProductVO();
//		productVO2.setProductID(1);
//		productVO2.setProdName("好香狗飼料");
//		productVO2.setPrice(800);
//		productVO2.setProductQuantity(8);
//		productVO2.setIntro("狗狗可以吃，健身仔也可以吃，真香");
//		dao.update(productVO2);
//
//		// 刪除
//		dao.delete(5);
//
//		// 查詢
//		ProductVO productVO3 = dao.findByPrimaryKey(1);
//		System.out.print(productVO3.getProductID() + ",");
//		System.out.print(productVO3.getProdName() + ",");
//		System.out.print(productVO3.getPrice() + ",");
//		System.out.print(productVO3.getProductQuantity() + ",");
//		System.out.print(productVO3.getIntro() + ",");
//		System.out.println("---------------------");

		// 查詢
//		List<ProductVO> list = dao.getAll();
//		for (ProductVO aProduct : list) {
//			System.out.print(aProduct.getProductID() + ",");
//			System.out.print(aProduct.getProdName() + ",");
//			System.out.print(aProduct.getPrice() + ",");
//			System.out.print(aProduct.getProductQuantity() + ",");
//			System.out.print(aProduct.getIntro() + ",");
//			System.out.println();
//		} 
	}
}