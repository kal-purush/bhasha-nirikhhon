package com.javalec.dto;

public class DtoSalesManage {
	int oseq;
	String eid;
	String customer_custid;
	String menu_menuid;
	String menuname;
	int menuprice;
	int oquantity;
	int ototal;
	String odate;
	


	public DtoSalesManage(int oseq, String eid, String customer_custid, String menu_menuid, String menuname,
			int menuprice, int oquantity, int ototal, String odate) {
		super();
		this.oseq = oseq;
		this.eid = eid;
		this.customer_custid = customer_custid;
		this.menu_menuid = menu_menuid;
		this.menuname = menuname;
		this.menuprice = menuprice;
		this.oquantity = oquantity;
		this.ototal = ototal;
		this.odate = odate;
	}

	public int getOseq() {
		return oseq;
	}

	public void setOseq(int oseq) {
		this.oseq = oseq;
	}

	public String getEid() {
		return eid;
	}

	public void setEid(String eid) {
		this.eid = eid;
	}

	public String getCustomer_custid() {
		return customer_custid;
	}

	public void setCustomer_custid(String customer_custid) {
		this.customer_custid = customer_custid;
	}

	public String getMenu_menuid() {
		return menu_menuid;
	}

	public void setMenu_menuid(String menu_menuid) {
		this.menu_menuid = menu_menuid;
	}

	public String getMenuname() {
		return menuname;
	}

	public void setMenuname(String menuname) {
		this.menuname = menuname;
	}

	public int getMenuprice() {
		return menuprice;
	}

	public void setMenuprice(int menuprice) {
		this.menuprice = menuprice;
	}

	public int getOquantity() {
		return oquantity;
	}

	public void setOquantity(int oquantity) {
		this.oquantity = oquantity;
	}

	public String getOdate() {
		return odate;
	}

	public void setOdate(String odate) {
		this.odate = odate;
	}
	
	public int getOtotal() {
		return ototal;
	}

	public void setOtotal(int ototal) {
		this.ototal = ototal;
	}


} // End