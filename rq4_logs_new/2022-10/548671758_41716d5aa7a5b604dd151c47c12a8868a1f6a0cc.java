package com.javalec.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import com.javalec.dto.DtoSalesManage;
import com.javalec.panel.AdminSalesPanel;
import com.javalec.panel.AdminSalesPanel;
import com.javalec.util.DBConnect;

public class AdminSalesFunction {

	// ---------------------------------- 모든 주문 출력
	// --------------------------------------

	public ArrayList<DtoSalesManage> selectListSales() {

		ArrayList<DtoSalesManage> dtoList = new ArrayList<DtoSalesManage>();

		String whereStatement = "select o.oseq, o.employee_eid, o.customer_custid, o.menu_menuid, m.menuname, m.menuprice, o.oquantity,m.menuprice*o.oquantity, o.odate ";
		String whereStatement2 = "from orders o, menu m ";
		String whereStatement3 = "where o.menu_menuid = m.menuid order by oseq";

		try {

			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection conn_mysql = DriverManager.getConnection(DBConnect.url_mysql, DBConnect.id_mysql,
					DBConnect.pw_mysql);

			Statement stmt_mysql = conn_mysql.createStatement();

			ResultSet rs = stmt_mysql.executeQuery(whereStatement + whereStatement2 + whereStatement3);

			while (rs.next()) {

				int wkOseq = rs.getInt(1);
				String wkEid = rs.getString(2);
				String wkCustomer_custid = rs.getString(3);
				String wkMenu_menuid = rs.getString(4);
				String wkMenuname = rs.getString(5);
				int wkMenuprice = rs.getInt(6);
				int wkOquantity = rs.getInt(7);
				int wkOtotal = rs.getInt(8);
				String wkOdate = rs.getString(9);
				DtoSalesManage dto = new DtoSalesManage(wkOseq, wkEid, wkCustomer_custid, wkMenu_menuid, wkMenuname,
						wkMenuprice, wkOquantity, wkOtotal, wkOdate);
				dtoList.add(dto);
			}

			conn_mysql.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dtoList;
	}

	// ---------------------------------- 기간 지정 검색 결과 출력
	// --------------------------------------

	public ArrayList<DtoSalesManage> selectListCalendar() {

		ArrayList<DtoSalesManage> dtoList = new ArrayList<DtoSalesManage>();

		String whereStatement = "select o.oseq, o.employee_eid, o.customer_custid, o.menu_menuid, m.menuname, m.menuprice, o.oquantity, m.menuprice*o.oquantity, o.odate ";
		String whereStatement2 = "from orders o, menu m ";
		String whereStatement3 = "where o.menu_menuid = m.menuid and odate between '"
				+ AdminSalesPanel.tfCalendar1.getText().trim() + "' and '" + AdminSalesPanel.tfCalendar2.getText().trim() + "' and "
				+ AdminSalesPanel.conditionQueryColumn + " like '%" + AdminSalesPanel.tfSelection.getText().trim()
				+ "%' order by oseq;";


		try {

			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection conn_mysql = DriverManager.getConnection(DBConnect.url_mysql, DBConnect.id_mysql,
					DBConnect.pw_mysql);

			Statement stmt_mysql = conn_mysql.createStatement();

			ResultSet rs = stmt_mysql.executeQuery(whereStatement + whereStatement2 + whereStatement3);

			while (rs.next()) {

				int wkOseq = rs.getInt(1);
				String wkEid = rs.getString(2);
				String wkCustomer_custid = rs.getString(3);
				String wkMenu_menuid = rs.getString(4);
				String wkMenuname = rs.getString(5);
				int wkMenuprice = rs.getInt(6);
				int wkOquantity = rs.getInt(7);
				int wkOtotal = rs.getInt(8);
				String wkOdate = rs.getString(9);
				DtoSalesManage dto = new DtoSalesManage(wkOseq, wkEid, wkCustomer_custid, wkMenu_menuid, wkMenuname,
						wkMenuprice, wkOquantity, wkOtotal, wkOdate);
				dtoList.add(dto);
			}

			conn_mysql.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dtoList;

	}

	// ---------------------------------- 조건 검색 결과 출력
	// --------------------------------------

	public ArrayList<DtoSalesManage> selectListSalesCondition() {

		ArrayList<DtoSalesManage> dtoList = new ArrayList<DtoSalesManage>();

		String whereStatement = "select o.oseq, o.employee_eid, o.customer_custid, o.menu_menuid, m.menuname, m.menuprice, o.oquantity, m.menuprice*o.oquantity, o.odate ";
		String whereStatement2 = "from orders o, menu m ";
		String whereStatement3 = "where o.menu_menuid = m.menuid and " + AdminSalesPanel.conditionQueryColumn;
		String whereStatement4 = " like '%" + AdminSalesPanel.tfSelection.getText().trim() + "%'  order by oseq";

		try {

			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection conn_mysql = DriverManager.getConnection(DBConnect.url_mysql, DBConnect.id_mysql,
					DBConnect.pw_mysql);

			Statement stmt_mysql = conn_mysql.createStatement();

			ResultSet rs = stmt_mysql
					.executeQuery(whereStatement + whereStatement2 + whereStatement3 + whereStatement4);

			while (rs.next()) {

				int wkOseq = rs.getInt(1);
				String wkEid = rs.getString(2);
				String wkCustomer_custid = rs.getString(3);
				String wkMenu_menuid = rs.getString(4);
				String wkMenuname = rs.getString(5);
				int wkMenuprice = rs.getInt(6);
				int wkOquantity = rs.getInt(7);
				int wkOtotal = rs.getInt(8);
				String wkOdate = rs.getString(9);
				DtoSalesManage dto = new DtoSalesManage(wkOseq, wkEid, wkCustomer_custid, wkMenu_menuid, wkMenuname,
						wkMenuprice, wkOquantity, wkOtotal, wkOdate);
				dtoList.add(dto);
			}

			conn_mysql.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dtoList;
	}

} // End