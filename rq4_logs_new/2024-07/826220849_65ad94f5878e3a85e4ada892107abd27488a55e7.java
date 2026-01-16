package com.tia102g3.productPic.model;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Product_PICJDBCDAO implements Product_PICDAO_interface {
	String driver = "com.mysql.cj.jdbc.Driver";
	String url = "jdbc:mysql://localhost:3306/tia102g3yun?serverTimezone=Asia/Taipei";
	String userid = "root";
	String passwd = "123456";

	private static final String INSERT_STMT = "INSERT INTO product_PIC (productID,pic) VALUES (?, ?)";
	private static final String GET_ALL_STMT = "SELECT productPicID, productID,pic FROM product_PIC order by productPicID";
	private static final String GET_ONE_STMT = "SELECT productPicID, productID,pic FROM product_PIC where productPicID = ?";
	private static final String DELETE = "DELETE FROM product_Pic where productPicID = ?";
	private static final String UPDATE = "UPDATE product_Pic set productID=?, pic=? where productPicID = ?";

	@Override
	public void insert(Product_PICVO product_PICVO) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(INSERT_STMT);
			pstmt.setInt(1, product_PICVO.getProductID());
//			byte[] pic = getPictureByteArray("image/science_pavlof_dog.png");
			pstmt.setBytes(2, product_PICVO.getPic());
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
	public void update(Product_PICVO product_PICVO) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(UPDATE);

			pstmt.setInt(1, product_PICVO.getProductID());
			pstmt.setBytes(2, product_PICVO.getPic());
			pstmt.setInt(3, product_PICVO.getProductPicID());

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
	public void delete(Integer productPicID) {

		Connection con = null;
		PreparedStatement pstmt = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(DELETE);

			pstmt.setInt(1, productPicID);

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
	public Product_PICVO findByPrimaryKey(Integer productPicID) {

		Product_PICVO product_PICVO = null;
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(GET_ONE_STMT);

			pstmt.setInt(1, productPicID);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				// empVo 也稱為 Domain objects
				product_PICVO = new Product_PICVO();
				product_PICVO.setProductPicID(rs.getInt("productPicID"));
				product_PICVO.setProductID(rs.getInt("productID"));
				product_PICVO.setPic(rs.getBytes("pic"));
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
		return product_PICVO;
	}

	@Override
	public List<Product_PICVO> getAll() {
		List<Product_PICVO> list = new ArrayList<Product_PICVO>();
		Product_PICVO product_PICVO = null;

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {

			Class.forName(driver);
			con = DriverManager.getConnection(url, userid, passwd);
			pstmt = con.prepareStatement(GET_ALL_STMT); // 將SQL指令傳送給資料庫
			rs = pstmt.executeQuery();

			while (rs.next()) {
				// empVO 也稱為 Domain objects
				product_PICVO = new Product_PICVO();
				product_PICVO.setProductPicID(rs.getInt("productPICID"));
				product_PICVO.setProductID(rs.getInt("productID"));
				product_PICVO.setPic(rs.getBytes("pic"));

				list.add(product_PICVO); // Store the row in the list
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

	public static void main(String[] args) throws IOException {

		Product_PICJDBCDAO dao = new Product_PICJDBCDAO();
		
		Connection con = null;
		PreparedStatement pstmt = null;
		


		// 新增
//		Product_PICVO product_PICVO1 = new Product_PICVO();
//		product_PICVO1.setProductID(1);
//		byte[] pic = getPictureByteArray("image/science_pavlof_dog.png");
//		product_PICVO1.setPic(pic);
//		dao.insert(product_PICVO1);
		
//
//		// 修改
		Product_PICVO product_PICVO2 = new Product_PICVO();
		product_PICVO2.setProductPicID(4);
		product_PICVO2.setProductID(1);
		byte[] pic = getPictureByteArray("image/dogfood.png");
		product_PICVO2.setPic(pic);
		dao.update(product_PICVO2);
//
//		// 刪除
//		dao.delete(3);
//
//		// 查詢
//		Product_PICVO product_PICVO3 = dao.findByPrimaryKey(3);
//		System.out.print(product_PICVO3.getProductPicID() + ",");
//		System.out.print(product_PICVO3.getProductID() + ",");
//		System.out.print(product_PICVO3.getPic() + ",");
//		System.out.println("---------------------");

		// 查詢
//		List<Product_PICVO> list = dao.getAll();
//		for (Product_PICVO aProduct_PIC : list) {
//			System.out.print(aProduct_PIC.getProductPicID() + ",");
//			System.out.print(aProduct_PIC.getProductID() + ",");
//			System.out.print(aProduct_PIC.getPic() + ",");
//
//			System.out.println();
//		}
	}
	
	public static byte[] getPictureByteArray(String path)throws IOException {

		FileInputStream fis = new FileInputStream(path);
		byte[] buffer = new byte[fis.available()];
		fis.read(buffer);
		fis.close();
		return buffer;

	}
}
