package movie.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MovieFeeDao extends DaoBase {
	public void insert(int movieId,String fee_type) {
		if(con==null) {
			return;
		}
		PreparedStatement stmt=null;
		try {
			String sql="insert into movie_fee(movie_id,fee_type) values(?,?)";
			stmt=con.prepareStatement(sql);
			stmt.setInt(2,movieId);
			stmt.setString(3, fee_type);
			stmt.executeUpdate();
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if(stmt!=null) {
					stmt.close();
					stmt=null;
				}
			}catch(SQLException e) {
				e.printStackTrace();
			}
		}
	}
}