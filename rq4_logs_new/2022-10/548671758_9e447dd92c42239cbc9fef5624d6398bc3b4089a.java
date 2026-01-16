package com.javalec.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import com.javalec.dto.DtoMenuManage;
import com.javalec.dto.DtoMenuManageManage;
import com.javalec.util.DBConnect;

public class DaoMenuManageManage {
	String mmendate;
	
	public ArrayList<DtoMenuManageManage> selectListAdminMenuManageManage() {

		ArrayList<DtoMenuManageManage> dtoList = new ArrayList<DtoMenuManageManage>();

		String whereStatement = "select *" + " from menumanage";
		try {

			Class.forName("com.mysql.cj.jdbc.Driver");

			Connection conn_mysql = DriverManager.getConnection(DBConnect.url_mysql, DBConnect.id_mysql,
					DBConnect.pw_mysql);

			// 입력할떄 필요없음 검색할떄 필요함
			Statement stmt_mysql = conn_mysql.createStatement();

			ResultSet rs = stmt_mysql.executeQuery(whereStatement);

			while (rs.next()) {

				String wkMenddate = rs.getString(5);

				DtoMenuManageManage dto = new DtoMenuManageManage(wkMenddate);
				dtoList.add(dto);
			}

			conn_mysql.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dtoList;

	}

}