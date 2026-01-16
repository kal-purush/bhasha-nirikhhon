package com.LinkeT.LinkeT.OrganizationChart.Dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.springframework.stereotype.Component;

import com.LinkeT.LinkeT.OrganizationChart.OrganizationChart;
import com.LinkeT.LinkeT.User.User;

@Component
public class OrganizationChartDaoImple implements OrganizationChartDao {
	
	private final String driver = "com.mysql.cj.jdbc.Driver";
 	private final String url = "jdbc:mysql://localhost:3306/LinkeT?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
 	private final String userId = "root";
 	private final String userPw = "root";
 	
 	private Connection conn = null;
 	private PreparedStatement pstmt = null;
 	private ResultSet rs = null;
	
	@Override
	public int joinTeam(String usrId, String teamCode, String usrGrade, String usrPart) {
		
		int result = 0;
		
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, userId, userPw);
			String sql = "insert into organizationchart (t_code, u_id, u_grade, u_part) values (?,?,?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1,teamCode);
			pstmt.setString(2,usrId);
			pstmt.setNString(3,usrGrade);
			pstmt.setString(4, usrPart);
			result = pstmt.executeUpdate();
			System.out.println(result);
		}catch(ClassNotFoundException e) {
			
			e.printStackTrace();
			return -1;
		}catch(SQLException e) {
			e.printStackTrace();
			return -1;
		}finally {	
			try {
				if (pstmt!=null) pstmt.close();
				if (conn!=null) conn.close();
			}catch(SQLException e) {
				e.printStackTrace();
			}
		}
		
		return result;
	}

	@Override
	public ArrayList<OrganizationChart> getUsrBelong(String usrId) {
		ArrayList<OrganizationChart> resultset = new ArrayList<OrganizationChart>();
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, userId, userPw);
			String sql = "select * from organizationchart where u_id = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1,usrId);
			rs = pstmt.executeQuery();

			
			while(rs.next()) {
				OrganizationChart org = new OrganizationChart();
				org.setOrganizationChart_serial((rs.getInt("org_serial")));
				org.setTeamCode(rs.getString("t_code"));
				org.setUsrId(rs.getString("u_id"));
				org.setUsrGrade(rs.getString("u_grade"));
				org.setUsrPart(rs.getString("u_part"));
				resultset.add(org);
			}
		}catch(ClassNotFoundException e) {
			e.printStackTrace();

		}catch(SQLException e) {
			e.printStackTrace();

		}finally {	
			try {
				if (pstmt!=null) pstmt.close();
				if (conn!=null) conn.close();
			}catch(SQLException e) {
				e.printStackTrace();
			}
		}
		
		return resultset;
	}

	@Override
	public ArrayList<User> getTeamUsrs(String teamCode) {
		// TODO Auto-generated method stub
		return null;
	}

}