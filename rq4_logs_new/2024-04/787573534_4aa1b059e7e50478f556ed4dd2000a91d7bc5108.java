package application.controller;

import application.Category;
import application.Location;
import application.Asset;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import application.CommonObjs;
import data_access_layer.DataAccessLayer;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.TextArea;
import javafx.scene.control.ComboBox;
import javafx.scene.control.DatePicker;

public class EditAssetController 
{
	private CommonObjs commonObjs = CommonObjs.getInstance();
	@FXML private TextField asset_name;
	@FXML private TextField cost;
	@FXML private ComboBox<String> categoryType;
	@FXML private ComboBox<String> locationType;
	@FXML private DatePicker purchase_date;
	@FXML private DatePicker warranty_date;
	@FXML private TextArea asset_descr;
	@FXML private Label result_message;
	private DataAccessLayer DAL = new DataAccessLayer();
	//private String selectedCategory;
	
	
	public void initialize()
	{
		DAL.storeCategoriesFromFile();
		DAL.storeLocationsFromFile();
		DAL.storeAssetsFromFile();
		
		HashMap<String, Category> categoriesMap = DAL.getCategoriesMap();
		HashMap<String, Location> locationsMap = DAL.getLocationsMap();
		
		List<String> categoryNames = new ArrayList<>(categoriesMap.keySet());
		List<String> locationNames = new ArrayList<>(locationsMap.keySet());
		
		
		categoryType.getItems().addAll(categoryNames);
		locationType.getItems().addAll(locationNames);
	}
	
	public void saveAsset(Asset asset)
	{
		categoryType.setPromptText(asset.getCategory());
		locationType.setPromptText(asset.getLocation());
		
	}
	
	@FXML public void saveAssetOp()
	{
		
	}

}