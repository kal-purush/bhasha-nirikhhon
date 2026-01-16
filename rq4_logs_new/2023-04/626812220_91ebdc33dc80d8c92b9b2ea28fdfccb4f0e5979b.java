package com.billi.model;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.billi.dbutil.OracleUtil;
import com.billi.vo.MembersVO;

public class MembersDAO {
	Connection conn;
	Statement st;
	PreparedStatement pst;
	ResultSet rs;
	int resultCount;
	CallableStatement cst;
	
	//회원가입
	public int membersInsert(MembersVO members) {
		String sql = """
				insert into members(mem_id, pw, mem_name, phone, address, nickname, grade) values(?, ?, ?, ?, ?, ?, ?)
				""";
		conn = OracleUtil.getConnection();
		
		System.out.println(members);
		try {
			pst = conn.prepareStatement(sql);
			
			pst.setString(1, members.getMem_id());
			pst.setString(2, members.getPw());
			pst.setString(3, members.getMem_name());
			pst.setString(4, members.getPhone());
			pst.setString(5, members.getAddress());
			pst.setString(6, members.getNickname());
			pst.setString(7, members.getGrade());
			
			resultCount = pst.executeUpdate();
		} catch (SQLException e) {
			resultCount = -1;
			e.printStackTrace();
		} finally {
			OracleUtil.dbDisconnect(null, pst, conn);
		}
		return resultCount;
	}
	
}