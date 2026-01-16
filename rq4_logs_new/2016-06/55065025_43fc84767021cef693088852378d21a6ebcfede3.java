package es.unizar.tmdad.dbconnecton;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import es.unizar.tmdad.dbmodel.AnalysisResource;
import es.unizar.tmdad.dbmodel.ResourceStatus;

public class AnalysisResourceDAO {

	public long createResource(AnalysisResource resource) {
		Connection conn = null;
		long resourceId = -1;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			String insertResourceSQL = "INSERT INTO " + DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE (book, user, state) VALUES (?,?,?)";
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(insertResourceSQL);
			pstmt.setLong(1, resource.getBookId());
			pstmt.setString(2, resource.getUsername());
			pstmt.setString(3, resource.getStatus().name());
			pstmt.executeUpdate();
			pstmt.close();
			
			String getResourceIdSQL = "SELECT id "
					+ "FROM " + DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE "
					+ "WHERE book=? AND user=? AND state=? AND ROWNUM=1"
					+ "ORDER BY id DESC";
			PreparedStatement pstmt2;
			pstmt2 = conn.prepareStatement(getResourceIdSQL);
			pstmt2.setLong(1, resource.getBookId());
			pstmt2.setString(2, resource.getUsername());
			pstmt2.setString(3, resource.getStatus().name());
			
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				resourceId = rs.getLong("id");
			}
			
			String insertTagSQL = "INSERT INTO " + DatabaseConstants.dbSchema + ".TAG (analysis, theme, term) VALUES (?,?,?)";
			PreparedStatement pstmt3;
			pstmt3 = conn.prepareStatement(insertTagSQL);
			
			Map<String, List<String>> tags = resource.getTag();
			String themeName;
			List<String> terms;
			String term;
			for (Iterator<String> iterator = tags.keySet().iterator(); iterator.hasNext();) {
				themeName = iterator.next();
				terms = tags.get(themeName);
				for (Iterator<String> iterator2 = terms.iterator(); iterator2.hasNext();) {
					term = (String) iterator2.next();
					pstmt3.setLong(1, resourceId);
					pstmt3.setString(2, themeName);
					pstmt3.setString(3, term);
					pstmt3.executeUpdate();
				}
			}
			pstmt3.close();
			
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
		return resourceId;
	}

	public AnalysisResource findResourceById(long id) {
		Connection conn = null;
		AnalysisResource resource = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			String getResourceSQL = "SELECT * "
					+ "FROM " + DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE "
					+ "INNER JOIN " + DatabaseConstants.dbSchema + ".TAG "
					+ "ON " + DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE.id=" + DatabaseConstants.dbSchema + ".TAG.analysis "
					+ "WHERE " + DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE.id=? "
					+ "ORDER BY " + DatabaseConstants.dbSchema + ".TAG.theme";
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(getResourceSQL);
			pstmt.setLong(1, id);
			
			ResultSet rs = pstmt.executeQuery();
			long bookId;
			String username;
			ResourceStatus status;
			String theme;
			String term;
			while (rs.next()) {
				if (resource==null) {
					bookId = rs.getLong(DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE.book");
					username = rs.getString(DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE.user");
					status = ResourceStatus.valueOf(rs.getString(DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE.state"));
					resource = new AnalysisResource(id, bookId, username, new HashMap<>());
					resource.setStatus(status);
				}
				theme = rs.getString(DatabaseConstants.dbSchema + ".TAG.theme");
				if (!resource.getTag().containsKey(theme)) {
					resource.getTag().put(theme, new ArrayList<>());
				}
				term = rs.getString(DatabaseConstants.dbSchema + ".TAG.term");
				resource.getTag().get(theme).add(term);
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
		return resource;
	}

	public void updateResourceStatusById(long id, ResourceStatus status) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			String insertResourceSQL = "UPDATE " + DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE SET state=? WHERE id=?";
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(insertResourceSQL);
			pstmt.setString(1, status.name());
			pstmt.setLong(2, id);
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

	public ResourceStatus findResourceStatusById(long id) {
		Connection conn = null;
		ResourceStatus status = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DatabaseConstants.dbUrl, DatabaseConstants.dbUser, DatabaseConstants.dbPass);
			
			String selectResourceStatusSQL = "SELECT state "
					+ "FROM " + DatabaseConstants.dbSchema + ".ANALYSIS_RESOURCE "
					+ "WHERE id=?";
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement(selectResourceStatusSQL);
			pstmt.setLong(1, id);
			
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				status = ResourceStatus.valueOf(rs.getString("state"));
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
		return status;
			
	}

	
}