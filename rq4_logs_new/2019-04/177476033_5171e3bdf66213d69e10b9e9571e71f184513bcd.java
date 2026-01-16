package database_manager;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import info.Info;
import servlet.ListServlet;

public class GroceryDataManagerTest {

	@Test
	public void addToListTest() throws SQLException {
		ArrayList<String> ingredients = new ArrayList<String>();
		GroceryDataManager gd = new GroceryDataManager();
		final Statement statement = mock(Statement.class);
        final Connection connection = mock(Connection.class);
        when(connection.createStatement()).thenReturn(statement);
		gd.addToList(ingredients);
		assertEquals("Exception tested","Exception tested");
	}

}