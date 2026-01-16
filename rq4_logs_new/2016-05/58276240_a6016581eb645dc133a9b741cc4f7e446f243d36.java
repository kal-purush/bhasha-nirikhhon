package main.test.mysqldao;

import static org.junit.Assert.assertNotNull;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import junit.framework.Assert;
import main.java.com.lab.restaurant.model.Mesa;
import main.java.com.lab.restaurant.utils.MySqlDBConexion;

@SuppressWarnings("deprecation")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MySQLMesaDaoTest {
	static int id;
	Connection cn;
	ResultSet rs;
	Mesa mesa;
	Mesa mesaUpdate;
	List<Mesa> lista;
	String sqlCreate = "{call USP_MESA_CREATE(?,?)}";
	String sqlRead = "{call USP_MESA_READ}";
	String sqlReadId = "{call USP_MESA_OBTAIN(?)}";
	String sqlUpdate = "{call USP_MESA_UPDATE(?,?)}";
	String sqlDelete = "{call USP_MESA_DELETE(?)}";

	@Before
	public void setUp() throws Exception {
		cn = MySqlDBConexion.getConexion();
		fillTestMesa();
	}
	
	private void fillTestMesa(){
		mesa = new Mesa(1, false);
		mesaUpdate = new Mesa(1, true);
	}

	@Test
	public void test1_ConnectionNotNull() {
		assertNotNull(cn);
	}
	
	
	@Test
	public void test2_SQLNotNull() throws Exception {
		
		assertNotNull(sqlCreate);
		assertNotNull(sqlRead);
		assertNotNull(sqlReadId);
		assertNotNull(sqlUpdate);
		assertNotNull(sqlDelete);
	}
	
	@Test
	public void test3_MesaCreation(){
		try{
			CallableStatement statement = cn.prepareCall(sqlCreate);
			statement.registerOutParameter(1, java.sql.Types.INTEGER);
			statement.setBoolean(2, mesa.isUsada());
			statement.executeUpdate();
			
			id = statement.getInt(1);
			mesa.setId(id);
			Assert.assertNotSame(0, id);
		}catch(SQLException ex){
			ex.printStackTrace();
		}
	}
	
	@Test
	public void test4_MesaRead(){
		try{
			mesa = null;
			CallableStatement statement = cn.prepareCall(sqlReadId);
			statement.setInt(1, id);
			rs = statement.executeQuery();
			if(rs.next()){
				mesa = new Mesa(rs.getInt(1), rs.getBoolean(2));
			}
		}catch(SQLException ex){
			ex.printStackTrace();
		}
		
		assertNotNull(mesa);
	}
	
	@Test
	public void test5_MesaReadAll() {
		try{
			CallableStatement statement = cn.prepareCall(sqlRead);
			rs = statement.executeQuery();
			lista = new ArrayList<Mesa>();
			while(rs.next()){
				Mesa mesa = new Mesa(rs.getInt(1), rs.getBoolean(2));
				lista.add(mesa);
			}
		}catch(SQLException ex){
			ex.printStackTrace();
		}
		
		assertNotNull(lista);
	}
	
	@Test
	public void test6_MesaUpdate() {
		try{
			CallableStatement statement = cn.prepareCall(sqlUpdate);
			statement.setInt(1, mesaUpdate.getId());
			statement.setBoolean(2, mesaUpdate.isUsada());
			Assert.assertEquals(1, statement.executeUpdate());
		}catch(SQLException ex){
			ex.printStackTrace();
		}
	}
	
	@Test
	public void test7_MesaDelete() {
		try{
			CallableStatement statement = cn.prepareCall(sqlDelete);
			statement.setInt(1, id);
			Assert.assertEquals(1, statement.executeUpdate());
		}catch(SQLException ex){
			ex.printStackTrace();
		}
	}


}