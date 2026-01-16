package application.controller;

import application.model.Item;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;

public class ItemController extends PalController{
	
	@FXML
	private CheckBox nameOfItem;
	@FXML
	private Label itemPriceLabel;
	@FXML
	private Label itemQuantityLabel;
	@FXML
	private Label totalAmountLabel;
	@FXML
	private Button removeItemBtn;
	
	private Item item;
	
	public void initializeItem(final Item item) {
		this.item = item;
		nameOfItem.setText(item.getItemName());
		itemPriceLabel.setText(String.valueOf(item.getPrice()));
		itemQuantityLabel.setText(String.valueOf(item.getAmount()));
		totalAmountLabel.setText(String.valueOf(item.getAmount() * item.getPrice()));
		
	}
	@FXML
	public void removeItem() {
		System.out.println("Remove Item");
	}

}