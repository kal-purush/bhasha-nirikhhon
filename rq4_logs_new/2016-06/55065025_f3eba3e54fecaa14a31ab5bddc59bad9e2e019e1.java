package es.unizar.tmdad.dbconnecton;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import es.unizar.tmdad.dbmodel.Book;
import es.unizar.tmdad.dbmodel.Chapter;

public class AnalysisDAO {

	public void insertAnalysis(Book b, Map<Chapter, Map<String, Integer>> analysis) {
		insertBook(b);
		for (Chapter c : analysis.keySet()) {
			insertChapter(b, c);
			for (String term : analysis.get(c).keySet()) {
				Integer count = analysis.get(c).get(term);
				insertAnalysis(b, c, term, count);
			}
		}
	}
	
	private boolean insertAnalysis(Book b, Chapter c, String term, Integer count) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			String insertBookSQL = "INSERT INTO THEMES_ANALYZER.ANALYSIS (book, chapter, term, num) VALUES (?,?,?,?)";
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(insertBookSQL);
			pstmt.setInt(1, b.getId());
			pstmt.setInt(2, c.getNum());
			pstmt.setString(3, term);
			pstmt.setInt(4, count);
			pstmt.executeUpdate();
			pstmt.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();			
			if (conn!=null) {
				try {
					conn.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
			return false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
	}

	private void insertChapter(Book b, Chapter c) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, "root", "root");
			String insertChapterSQL = "INSERT INTO THEMES_ANALYZER.CHAPTER (book, num, title) VALUES (?,?,?)";
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(insertChapterSQL);
			pstmt.setInt(1, b.getId());
			pstmt.setInt(2, c.getNum());
			pstmt.setString(3, c.getTitle());
			pstmt.executeUpdate();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();			
			if (conn!=null) {
				try {
					conn.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private boolean insertBook(Book b) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			String insertBookSQL = "INSERT INTO THEMES_ANALYZER.BOOK (id, title) VALUES (?,?)";
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(insertBookSQL);
			pstmt.setInt(1, b.getId());
			pstmt.setString(2, b.getTitle());
			pstmt.executeUpdate();
			pstmt.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();			
			if (conn!=null) {
				try {
					conn.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
			return false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
	}

	public Map<Chapter, Map<String, Integer>> getAnalysisFromBook(Book b) {
		// TODO Auto-generated method stub
		return null;
	}
	
}