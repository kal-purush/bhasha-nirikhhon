package rw.member.model.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import rw.common.JDBCTemplate;
import rw.member.model.vo.FileData;

public class FileDAO {

	public int uploadFile(Connection conn, FileData fd) {
		PreparedStatement pstmt = null;
		int result = 0;
		
		String query = "update member set profile_img=? where member_id=?";
		try {
			pstmt = conn.prepareStatement(query);
			pstmt.setString(1, fd.getChangedFileName());
			pstmt.setString(2, fd.getFileUser());
			result = pstmt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			JDBCTemplate.close(pstmt);
		}
		return result;
	}
}