package client;

import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;

public class GrandTichuView extends BorderPane { 
	protected Button yesButton;
	protected Button noButton;
	protected Label infoLAbel;
	
	public GrandTichuView() {
		this.yesButton=new Button("Yes");
		this.noButton=new Button("No");
		this.infoLAbel=new Label("Do You Want To Say Grand Tichu?");
		
		this.setTop(this.infoLAbel);
		this.setLeft(this.yesButton);
		this.setRight(this.noButton);
	}
}