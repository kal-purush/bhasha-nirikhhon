package application.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import application.model.Item;
import application.model.ShoppingList;
import application.model.User;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Button;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.Pane;

public class HomeController extends PalController{
	
	@FXML
	private Button shoppingListBtn;
	@FXML
	private Button profileBtn;
	@FXML
	private Button addListBtn;
	@FXML
	private FlowPane shoppingListPane;
	
	public void initializeHome(final User user) throws IOException {
	
		final List<ShoppingList> shoppingLists = getShoppingList(user);
		
		
		
		//for (ShoppingList list : shoppingLists) {
		
		//remove this counter and replace with shopping lists
		for(int i = 0; i < 3; i++) {
			
			FXMLLoader loader = new FXMLLoader();
			loader.setLocation(this.getClass().getClassLoader().getResource("application/view/ShoppingList.fxml"));
			Pane pane = (Pane) loader.load();
			
			Item item = new Item("pizza", 32.50, 3);
			List<Item> items = new ArrayList<>();
			items.add(item);
			
			ShoppingList list = new ShoppingList();
			list.setDateForList(new Date());
			list.setItems(items);
			list.setListName("List " + i);
			

			ShoppingListController shoppingListController = loader.getController();
			shoppingListController.initializeController(list);
			shoppingListPane.getChildren().add(pane);
		}
	}
	
	private List<ShoppingList> getShoppingList(final User user) {
		
		return new ArrayList<>();
	}

	@FXML
	public void loadProfile(final ActionEvent e) {
		System.out.println("Load Profile");
	}
	
	@FXML
	public void loadShoppingLists(final ActionEvent e) {
		System.out.println("Load Shopping Lists");
	}
	
	@FXML
	public void addShoppingList(final ActionEvent e) {
		System.out.println("Add shopping lists");
	}
}