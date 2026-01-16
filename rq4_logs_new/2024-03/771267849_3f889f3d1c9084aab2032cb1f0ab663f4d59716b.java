package MiniProject.Record;

import java.sql.Date;

public class Record {
	private int num;
	private int account_num;
	private int money;
	private String name;
	private Date newDate;
	
	public Record() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Record(int num, int account_num, int money, String name, Date newDate) {
		super();
		this.num = num;
		this.account_num = account_num;
		this.money = money;
		this.name = name;
		this.newDate = newDate;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public int getAccount_num() {
		return account_num;
	}

	public void setAccount_num(int account_num) {
		this.account_num = account_num;
	}

	public int getMoney() {
		return money;
	}

	public void setMoney(int money) {
		this.money = money;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getNewDate() {
		return newDate;
	}

	public void setNewDate(Date newDate) {
		this.newDate = newDate;
	}

	@Override
	public String toString() {
		return "Record [num=" + num + ", account_num=" + account_num + ", money=" + money + ", name=" + name
				+ ", newDate=" + newDate + "]";
	}
	
	

} 