package es.unizar.tmdad.analyzer.services.db.connecton;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import es.unizar.tmdad.analyzer.services.db.model.BookAnalysis;
import es.unizar.tmdad.analyzer.services.db.model.ChapterAnalysis;

public class AnalysisDAO {

	public void createAnalysis(long bookId, long chapterNum, String term, long count) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			String insertResultSQL = "INSERT INTO " + DatabaseConstants.dbSchema + ".RESULT(book, chapter, term, termCount) VALUES (?,?,?,?)";
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(insertResultSQL);
			pstmt.setLong(1, bookId);
			pstmt.setLong(2, chapterNum);
			pstmt.setString(3, term);
			pstmt.setLong(4, count);
			pstmt.executeUpdate();
			pstmt.close();
			conn.close();
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

	public long getAnalysisOfTerm(long bookId, long chapterNum, String term) {
		Connection conn = null;
		long count = -1;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			String selectBookSQL = "SELECT termCount "
					+ "FROM " + DatabaseConstants.dbSchema + ".RESULT "
					+ "WHERE book=? AND chapter=? AND term=?";

			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(selectBookSQL);
			pstmt.setLong(1, bookId);
			pstmt.setLong(2, chapterNum);
			pstmt.setString(3, term);
			
			ResultSet rs = pstmt.executeQuery();

			if (rs.next()) {
				count = rs.getLong("termCount");
			}				
			pstmt.close();
			
			conn.close();
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
		return count;
	}

	public BookAnalysis findAnalysisByBookAndTerms(long bookId, List<String> terms) {
		Connection conn = null;
		BookAnalysis bookAnalysis = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			String selectAnalysisSQL = "SELECT * "
					+ "FROM " + DatabaseConstants.dbSchema + ".RESULT "
					+ "INNER JOIN " + DatabaseConstants.dbSchema + ".BOOK "
					+ "ON " + DatabaseConstants.dbSchema + ".RESULT.book=" + DatabaseConstants.dbSchema + ".BOOK.id "
					+ "INNER JOIN " + DatabaseConstants.dbSchema + ".CHAPTER "
					+ "ON " + DatabaseConstants.dbSchema + ".RESULT.chapter=" + DatabaseConstants.dbSchema + ".CHAPTER.num "
					+ "AND " + DatabaseConstants.dbSchema + ".RESULT.book=" + DatabaseConstants.dbSchema + ".CHAPTER.book "
					+ "WHERE " + DatabaseConstants.dbSchema + ".BOOK.id=? AND (" + DatabaseConstants.dbSchema + ".RESULT.term=? ";

			for (int i = 1; i < terms.size(); i++) {
				selectAnalysisSQL += "OR " + DatabaseConstants.dbSchema + ".RESULT.term=? "; 
			}
			selectAnalysisSQL += ") ORDER BY " + DatabaseConstants.dbSchema + ".CHAPTER.num ";
			
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(selectAnalysisSQL);	
			pstmt.setLong(1, bookId);
			
			for (int i = 0; i < terms.size(); i++) {
				String term = terms.get(i);
				pstmt.setString(i+2, term);
			}
			
			ResultSet rs = pstmt.executeQuery();

			int chapterNum;
			String chapterTitle;
			ChapterAnalysis chapter;
			long count;
			String term;
			while (rs.next()) {
				if (bookAnalysis==null) {
					String bookTitle = rs.getString("title");
					bookAnalysis = new BookAnalysis(bookId, bookTitle, new ArrayList<ChapterAnalysis>());
				}
				chapterNum = rs.getInt("num");
				chapterTitle = rs.getString("chapterTitle");
				term = rs.getString("term");
				count = rs.getLong("termCount");
				if (bookAnalysis.getChapters().size()<chapterNum) {
					chapter = new ChapterAnalysis(chapterNum, chapterTitle, new HashMap<String, Long>());
					bookAnalysis.getChapters().add(chapter);
				} else {
					chapter = bookAnalysis.getChapters().get(chapterNum-1);	
				} 
				chapter.getCounts().put(term, count);
			}				
			pstmt.close();
			
			conn.close();
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
		return bookAnalysis;
	}
	
}