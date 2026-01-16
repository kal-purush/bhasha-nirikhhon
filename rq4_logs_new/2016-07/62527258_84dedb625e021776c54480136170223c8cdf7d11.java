package daoImpl.mock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;


import dao.ScheduleDao;

import model.ScheduleOfClasses;

import util.DBUtil;

public class ScheduleSQLDao implements ScheduleDao {

	@Override
	public ScheduleOfClasses getScheduleOfClass(int semester) {
		// TODO Auto-generated method stub
		
			ScheduleOfClasses schedule=null;
			
			Connection conn=DBUtil.getConnection();
			Statement stmt=DBUtil.getStatement(conn);
			String sql="select * from ScheduleOfClasses where semester=" + semester;
			ResultSet rs=DBUtil.getResultSet(stmt, sql);
			HashMap<String, String> sectionsOffered=new HashMap<String,String>();
			try{
				while(rs.next()){
					sectionsOffered.put(String.valueOf(rs.getInt("ID")).toString(),rs.getString("courseName"));
					
				}
			}catch(SQLException e){
				e.printStackTrace();
			}finally{
				DBUtil.close(stmt);
				DBUtil.close(rs);
				DBUtil.close(conn);
			}
			schedule=new ScheduleOfClasses(semester,sectionsOffered);
		return schedule;
	}
	

}