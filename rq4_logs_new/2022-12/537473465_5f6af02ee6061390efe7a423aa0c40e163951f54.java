package householdhero;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import group4.householdhero.controller.BudgetController;
import group4.householdhero.controller.Controller;
import group4.householdhero.model.Budget;
import group4.householdhero.model.Product;
import javafx.application.Platform;
import javafx.scene.chart.PieChart;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableView;

@TestInstance(Lifecycle.PER_CLASS)
public class BudgetControllerJavaFXTest extends BudgetController {
	
	@BeforeAll
	public void initJfxRuntime() {
		Platform.startup(() -> {});
	}
	
	@BeforeAll
	public void setController() {
		controller = mock(Controller.class);
		when(controller.getLocalisedString("english.choice.text")).thenReturn("English");
		when(controller.getLocalisedString("gaeilge.choice.text")).thenReturn("Gaeilge");
		when(controller.getLocalisedString("budget.distribution.fridge.text")).thenReturn("Fridge");
        when(controller.getLocalisedString("budget.distribution.used.text")).thenReturn("Used");
        when(controller.getLocalisedString("budget.distribution.waste.text")).thenReturn("Waste");
		when(controller.getLocalisedString("remaining.budget.text")).thenReturn("Remaining Budget (€)");
		when(controller.setLocale("en_IE")).thenReturn(new Locale("en_IE"));
		when(controller.setLocale("ga_IE")).thenReturn(new Locale("ga_IE"));
	}
	
	@Test
	public void setLanguageChoiceBoxTest() {
		languageChoiceBox = new ChoiceBox<String>();
		when(controller.getLocale()).thenReturn(new Locale("en_IE"));
		setLanguageChoiceBox();
		assertEquals("English", languageChoiceBox.getValue(), "The value of languageChoiceBox is incorrect");
		
		languageChoiceBox = new ChoiceBox<String>();
		when(controller.getLocale()).thenReturn(new Locale("ga_IE"));
		setLanguageChoiceBox();
		assertEquals("Gaeilge", languageChoiceBox.getValue(), "The value of languageChoiceBox is incorrect");
		
		assertEquals(Arrays.asList("English", "Gaeilge"), languageChoiceBox.getItems(), "The items in languageChoicebox were incorrect");
	}
	
	@Test
	public void changeSelectedLanguageTest() throws IOException {
		languageChoiceBox = new ChoiceBox<String>();
		when(controller.getLocale()).thenReturn(new Locale("en_IE"));
		setLanguageChoiceBox();
		
		languageChoiceBox.setValue("English");
		assertEquals("en_ie", changeSelectedLanguage().toString(), "The selected language was incorrect");
		
		languageChoiceBox.setValue("Gaeilge");
		assertEquals("ga_ie", changeSelectedLanguage().toString(), "The selected language was incorrect");
	}
	
	@Test
	public void setBudgetInformationTest() {
		startDateLabel = new Label();
		endDateLabel = new Label();
		plannedBudgetLabel = new Label();
		spentBudgetLabel = new Label();
		remainingBudgetLabel = new Label();
		
		budget = mock(Budget.class);
		when(budget.getStartDate()).thenReturn(LocalDate.parse("2000-01-01"));
		when(budget.getEndDate()).thenReturn(LocalDate.parse("2000-12-31"));
		when(budget.getPlannedBudget()).thenReturn(100.0);
		when(budget.getSpentBudget()).thenReturn(25.0);
		
		setBudgetInformation();
		
		assertEquals("2000-01-01", startDateLabel.getText(), "Start date was incorrect");
		assertEquals("2000-12-31", endDateLabel.getText(), "End date was incorrect");
		assertEquals("100.0", plannedBudgetLabel.getText(), "Planned budget was incorrect");
		assertEquals("25.0", spentBudgetLabel.getText(), "Spent budget was incorrect");
		assertEquals("75.0", remainingBudgetLabel.getText(), "Remaining budget was incorrect");
	}
	
	@Test
	public void setPieChartTest() {
		pieChart = new PieChart();
		fridgeTotalCost = 1.0;
		usedTotalCost = 2.0;
		wasteTotalCost = 3.0;
		
		budget = mock(Budget.class);
		when(budget.getPlannedBudget()).thenReturn(100.0);
		when(budget.getSpentBudget()).thenReturn(25.0);
		
		setPieChart();
		assertEquals("[Data[Fridge,1.0], Data[Used,2.0], Data[Waste,3.0], Data[Remaining Budget (€),75.0]]", pieChart.getData().toString(), "The pie chart contents were incorrect");
	}
	
	@Test
	public void getBudgetsTest() throws SQLException {
		budgetChoiceBox = new ChoiceBox<Budget>();
		List<Budget> list = new ArrayList<Budget>();
		list.add(mock(Budget.class));
		list.add(mock(Budget.class));
		list.add(mock(Budget.class));
		
		when(controller.getAllBudgets()).thenReturn(list);
		
		getBudgets();
		assertEquals(3, budgetChoiceBox.getItems().size(), "The choice box items are incorrect");
	}
	
	@Test
	public void setProductsTest() throws SQLException {
		productsDuringBudgetTable = new TableView<Product>();
		
		budget = mock(Budget.class);
		when(budget.getId()).thenReturn(0);
		
		List<Product> list = new ArrayList<Product>();
		list.add(mock(Product.class));
		list.add(mock(Product.class));
		list.add(mock(Product.class));
		
		when(controller.getProductsByBudget(0)).thenReturn(list);
		
		setProducts();
		
		assertEquals(3, productsDuringBudgetTable.getItems().size(), "The table items are incorrect");
	}
}