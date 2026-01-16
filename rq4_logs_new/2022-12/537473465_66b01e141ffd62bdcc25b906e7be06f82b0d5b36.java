package householdhero;

import group4.householdhero.model.CategoryLocaliser;
import group4.householdhero.model.Localiser;
import group4.householdhero.model.Model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class CategoryLocaliserTest {
	CategoryLocaliser localiser;
	
	@BeforeAll
	public void createLocaliser() throws SQLException {
		ArrayList<String> categories = new ArrayList<String>();
		categories.add("category.baked.goods.text");
		categories.add("category.bread.text");
		categories.add("category.candy.text");
		
		Model model = mock(Model.class);
		when(model.getCategories()).thenReturn(categories);
		localiser = new CategoryLocaliser(model, new Localiser());
	}
	
	@Test
	public void localiseCategoriesTest() {
		localiser.localizeCategories();
		ArrayList<String> localisedCategories = (ArrayList<String>) localiser.getLocalizedCategories();
		
		assertEquals("[Baked Goods, Bread, Candy]", localisedCategories.toString(), "The categories are incorrect");
	}
	
	@Test
	public void unlocaliseCategory() {
		localiser.localizeCategories();
		String string = localiser.unlocaliseCategory("Baked Goods");
		assertEquals("category.baked.goods.text", string, "The string was incorrect");
	}
}