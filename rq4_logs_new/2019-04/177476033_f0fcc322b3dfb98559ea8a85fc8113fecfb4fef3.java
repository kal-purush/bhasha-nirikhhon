package database_manager;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Ignore;
import org.junit.Test;

import info.Info;
import info.RecipeInfo;
import info.RestaurantInfo;

public class OrderDataManagerTest {
	
	@Ignore
	@Test
	public void restaurantSetOrderTest() throws SQLException {
		RestaurantDataManager rdm = new RestaurantDataManager("nero");
		RestaurantInfo info = new RestaurantInfo("", 0, "testid", "", 0, "", 0, "", "");
		rdm.addToList(info, 1);
		OrderDataManager odm = new OrderDataManager("nero");
		info.order = 1;
		odm.setOrder(info);
		ArrayList<Info> list = rdm.loadRestaurants(1);
		for(Info i : list) {
			RestaurantInfo iRes = (RestaurantInfo)i;
			if(iRes.placeID.equals("testid"))
				assertTrue(iRes.order == 1);
		}
		rdm.removeFromList("testid", 1);
	}

	@Ignore
	@Test
	public void recipeSetOrderTest() throws SQLException {
		RecipeDataManager rdm = new RecipeDataManager("nero");
		RecipeInfo info = new RecipeInfo("", 0, 1234321, 0, 0, new ArrayList<String>(),new ArrayList<String>(), "");
		rdm.addToList(info, 1);
		OrderDataManager odm = new OrderDataManager("nero");
		info.order = 1;
		odm.setOrder(info);
		ArrayList<Info> list = rdm.loadRecipes(1);
		for(Info i : list) {
			RecipeInfo iReci = (RecipeInfo)i;
			if(iReci.spoonID == 1234321)
				assertTrue(iReci.order == 1);
		}
		rdm.removeFromList(1234321, 1);
	}
}