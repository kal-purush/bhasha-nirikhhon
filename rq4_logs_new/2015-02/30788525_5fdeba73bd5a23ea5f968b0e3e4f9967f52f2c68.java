package com.common.dbutil;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * dao的JDBC实现
 * @author hyq
 *
 */
public class DaoJdbcImpl implements Dao{
	
	static{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	private String host,user,password,database,charset;
	int port;
	
	public DaoJdbcImpl(String host,String user,String password,String database,int port){
		this.password=password;
		this.port=port;
		this.host=host;
		this.user=user;
		this.database=database;
		this.charset="UTF8";
	}
	
	/**
	 * 执行查询，并返回查询结果的唯一值
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	public Object executeQuerySingle(String sql,Object... parameters) throws Exception{
		List<Map<String,Object>> list=(List<Map<String,Object>>)this.executeQuery(sql, parameters);
		if(list==null || list.size()==0)
			return null;
		Map<String,Object> map=list.get(0);
		if(map==null || map.isEmpty())
			return null;
		return map.values().iterator().next();
		
	}
	
	public Connection getConnection() throws Exception {
		return DriverManager.getConnection("jdbc:mysql://"+host
				+":"+port+"/"+database+"?user="+user+"&password="+password
				+"&charset="+charset);
	}
	@Override
	public List executeQuery(String sql,Object... parameters) throws Exception{
		Connection con=this.getConnection();
		PreparedStatement ps=con.prepareStatement(sql);
		for(int i=0;i<parameters.length;i++){
			ps.setObject(i, parameters[i]);
		}
		ResultSet rs=ps.executeQuery();
		List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
		/*
		 * 循环取出结果集的数据行，并把每行数据封装成一个以列名为KEY，列值为值的MAP对象；最后所有行的数据再放
		 * 入LISt
		 */
		while(rs.next()){
			Map<String,Object> map=new HashMap<String, Object>();
			int columnCount=rs.getMetaData().getColumnCount();
			for(int i=0;i<columnCount;i++){
				map.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
			}
			list.add(map);
		}
		rs.close();
		ps.close();
		con.close();
		return list;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getCharset() {
		return charset;
	}
	public void setCharset(String charset) {
		this.charset = charset;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	@Override
	public void add(Object entity) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void delete(Object entity) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void update(Object entity) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Object getById(Serializable id) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List getAll() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List getAll(Paging paging) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Object getByName(String name) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List queryByPropertys(String[] propertyNames, int[] opFlags,
			int[] conditionFlag, Object[] values) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List queryByPropertys(String[] propertyNames, int[] opFlags,
			int[] conditionFlag, Object[] values, Paging paging)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List queryByProperty(String fieldName, int opFlag, Object value)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List queryByProperty(String fieldName, int opFlag, Object value,
			Paging paging) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public int executeUpdate(String sql, Object... parameters) throws Exception {
		Connection con=this.getConnection();
		PreparedStatement ps=con.prepareStatement(sql);
		for(int i=0;i<parameters.length;i++){
			ps.setObject(i, parameters[i]);
		}
		int rs=ps.executeUpdate();
		ps.close();
		con.close();
		return rs;
	}
	@Override
	public List executeQuery(String sql, Paging paging, Object... parameters)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List executeQuery(String sql) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List executeQuery(String sql, Paging paging) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}