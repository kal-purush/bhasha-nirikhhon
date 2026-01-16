package application;

import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;

public class Dashboard extends BorderPane {

	public Dashboard() {
		super();
		init();
	}
	
	private void init() {
		initLeft();
		initRight();
		initTop();
		initBottom();
		initCenter();
	}
	
	private void initLeft() {
		Label testLeft = new Label("Test left");
		this.setLeft(testLeft);
	}
	
	private void initRight() {
		Label testRight = new Label("Test right");
		this.setRight(testRight);
	}
	
	private void initTop() {
		Label testTop = new Label("Test top");
		this.setTop(testTop);
	}
	
	private void initBottom() {
		Label testBot = new Label("Test bottom");
		this.setBottom(testBot);
	}
	
	private void initCenter() {
		Label testCenter = new Label("Test center");
		this.setCenter(testCenter);
	}
}