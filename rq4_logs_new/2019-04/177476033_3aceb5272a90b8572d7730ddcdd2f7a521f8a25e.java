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

import info.GroceryInfo;
import info.Info;
import info.RecipeInfo;

public class RecipeDataManagerTest {

	@Test
	public void addToListAndLoadRecipesTest() throws SQLException {
		RecipeDataManager manager = new RecipeDataManager();
		manager.addToList(new RecipeInfo("", 1, 1, 0, 0, new ArrayList<String>(), new ArrayList<String>(), ""), 1);
		ArrayList<Info> list = manager.loadRecipes(1);
		assert(list.size() > 0);
	}
	
}