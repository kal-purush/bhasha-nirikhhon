package application;

import java.sql.*;

import javax.servlet.http.HttpServletRequest;

import com.viettel.config.DBConnection;

public class LoginDao {

	public static boolean validate(LoginBean bean, HttpServletRequest request)
			throws SQLException {
		boolean status = false;
		Connection con = null;
		try {
			con = DBConnection.getInstance().getConnect(request);

			PreparedStatement ps = con
					.prepareStatement("select * from APP.USER where username=? and password=?");

			ps.setString(1, bean.getUsername());
			ps.setString(2, bean.getPassword());

			ResultSet rs = ps.executeQuery();
			status = rs.next();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con != null)
				con.close();
		}

		return status;

	}
}