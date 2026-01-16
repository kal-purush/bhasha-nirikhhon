package com.fccs.dp.structure.decorator;

public class Client {

	public static void main(String[] args) {
		DataSource dataSource = new DataSource();
		System.out.println(dataSource.getSize());
		
		Conn conn = dataSource.getConn();
		System.out.println(dataSource.getSize());
		
		conn.close();
		System.out.println(dataSource.getSize());
	}
	
}