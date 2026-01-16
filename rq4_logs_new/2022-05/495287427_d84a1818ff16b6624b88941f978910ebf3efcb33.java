package mvc.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;

public class addshopDAO {
	
	private static final Connection DBConnection = null;
	private static addshopDAO instance;
	
	private addshopDAO() {} 
	
	public static addshopDAO getInstance() {
		if(instance == null) {
			instance = new addshopDAO();
		}
		return instance;
	}
	
	// 회원정보 추가하기
	
	public void insertMember(HttpServletRequest request) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql;
		
		try 
		{
			String name = request.getParameter("name");
			String stime = request.getParameter("joinid1");
			String ftime = request.getParameter("joinid2");
			String phone = request.getParameter("phone1") + request.getParameter("phone2") + request.getParameter("phone3");
			String address = request.getParameter("join_address1") + request.getParameter("join_address2") + request.getParameter("join_address3");
			String[] category_group = request.getParameterValues("sectors");
			String category = String.join(",",category_group);
			String menu_textarea = request.getParameter("menu_textarea");
			String store_self = request.getParameter("store_self");
			
			conn = DBConn.dbconn();
			sql = "insert into addstore(s_name,s_stime,s_ftime,m_phone,s_adress,s_category,s_Mtext,s_MItext) values(?,?,?,?,?,?,?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, name);
			pstmt.setString(2, stime);
			pstmt.setString(3, ftime);
			pstmt.setString(4, phone);
			pstmt.setString(5, address);
			pstmt.setString(6, category);
			pstmt.setString(7, menu_textarea);
			pstmt.setString(8, store_self);
			
			pstmt.executeUpdate();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try 
			{
				
			if(pstmt != null) 
			{
				pstmt.close();
			}
			if(conn != null) 
			{
				conn.close();
			}
			}
			catch(Exception e) 
			{
				e.printStackTrace();
			}
		}
		}
}