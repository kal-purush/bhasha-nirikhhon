package application;

import java.time.Month;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.layout.GridPane;

public class GetMilkDialog extends Dialog<MilkList>{
	private ObservableList<Month> months = FXCollections.observableArrayList(
			Month.JANUARY,
			Month.FEBRUARY,
			Month.MARCH,
			Month.APRIL,
			Month.MAY,
			Month.JUNE,
			Month.JULY,
			Month.AUGUST,
			Month.SEPTEMBER,
			Month.OCTOBER,
			Month.NOVEMBER,
			Month.DECEMBER);
	
	public GetMilkDialog(MilkList milkList) {
		this.setTitle("Get Milk");
		
		ButtonType insertButtonType = new ButtonType("Retrieve", ButtonData.OK_DONE);
		this.getDialogPane().getButtonTypes().addAll(insertButtonType, ButtonType.CANCEL);
		this.setHeaderText("Please enter parameters to search from:");
		GridPane grid = new GridPane();
		grid.setHgap(10);
		grid.setVgap(10);
		grid.setPadding(new Insets(20, 150, 10, 10));
		
		//creation of boxes to return the information requested
		ComboBox<Month> startMonth = new ComboBox<>(months);
		ComboBox<Month> endMonth = new ComboBox<>(months);
		TextField startYear = new TextField();
		TextField endYear = new TextField();
		TextField farm = new TextField();
		
		//adding boxes to grid
		grid.add(new Label("Farm Name:"), 0, 0);
		grid.add(new Label("Starting Year:"), 0, 1);
		grid.add(new Label("Ending Year:"), 0, 2);
		grid.add(new Label("Starting Month"), 0, 3);
		grid.add(new Label("Ending Month"), 0, 4);
		
		grid.add(farm, 1, 0);
		grid.add(startYear, 1, 1);
		grid.add(endYear, 1, 2);
		grid.add(startMonth, 1, 3);
		grid.add(endMonth, 1, 4);
		
		this.getDialogPane().setContent(grid);
		
		Node loginButton = this.getDialogPane().lookupButton(insertButtonType);
		loginButton.setDisable(false);
		
		this.setResultConverter(dialogButton -> {
			MilkList specificMilkList = null;
			if (dialogButton == insertButtonType) {
				int startingMonth;
				int endingMonth;
				int startingYear;
				int endingYear;
				String farmName;
				if(startMonth.getValue() == null) {
					startingMonth = 0;
				}else {
					startingMonth = startMonth.getValue().getValue();
				}
				if(endMonth.getValue() == null) {
					endingMonth = 12;
				}else {
					endingMonth = endMonth.getValue().getValue();
				}
				if(startYear.getText().equals("")) {
					startingYear = 0;
				}else {
				startingYear = Integer.parseInt(startYear.getText());
				}
				if(endYear.getText().equals("")) {
					endingYear = -1;
				}else {
					endingYear = Integer.parseInt(endYear.getText());
				}
				farmName = farm.getText();
				specificMilkList = new MilkList();
				//over the course of only one year
				if(startingYear == endingYear || endingYear == -1) {
					specificMilkList.addAll(milkList.getFromMonths(startingYear, startingMonth, endingMonth).getFromFarm(farmName));
				}
				//over two simultaneous years
				else if(startingYear+1 == endingYear) {
					specificMilkList.addAll(milkList.getFromMonths(startingYear, startingMonth, 12).getFromFarm(farmName));
					specificMilkList.addAll(milkList.getFromMonths(endingYear, 0, endingMonth).getFromFarm(farmName));
				}
				// >2 years
				else {
					specificMilkList.addAll(milkList.getFromMonths(startingYear, startingMonth, 12).getFromFarm(farmName));
					specificMilkList.addAll(milkList.getFromMonths(endingYear, 0, endingMonth).getFromFarm(farmName));
					specificMilkList.addAll(milkList.getFromYears(startingYear+1, endingYear-1)
							.getFromFarm(farmName));
					
				}
				MilkStatsTable table = new MilkStatsTable(specificMilkList);
				Dialog<MilkList> dialog = new Dialog<>();
				dialog.getDialogPane().setContent(table);
				ButtonType okButtonType = new ButtonType("Ok", ButtonData.OK_DONE);
				dialog.getDialogPane().getButtonTypes().addAll(okButtonType, ButtonType.CANCEL);
				dialog.show();
				return specificMilkList;
				
			}
			return null;
		});
	}
	
	
}