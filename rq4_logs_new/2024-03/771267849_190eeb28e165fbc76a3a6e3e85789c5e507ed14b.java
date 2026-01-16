package Accounting;

import java.sql.Date;

public class Account {
	//계좌번호
	private int account_num;
	//계좌주 id
	private String id;
	//금액(잔액)
	private int num;
	//가입일
	private Date date;
	//예금:승인여부
	//적금:만기일
	
	public Account() {}
	
	public Account(int account_num, String id, int num, Date date) {
		this.account_num = account_num;
		this.id = id;
		this.num = num;
		this.date = date;
	}
	
	public int getAccount_num() {
		return account_num;
	}

	public void setAccount_num(int account_num) {
		this.account_num = account_num;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return "Account [account_num=" + account_num + ", id=" + id + ", num=" + num + ", date=" + date + "]";
	}
		
}