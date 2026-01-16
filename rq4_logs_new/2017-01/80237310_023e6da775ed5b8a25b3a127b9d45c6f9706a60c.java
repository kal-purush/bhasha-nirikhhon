import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;


public class Action10Event implements ActionListener {
	private ActionController _actionController;

	public Action10Event(ActionController actionController){
		this._actionController=actionController;
	}

	public void actionPerformed(ActionEvent arg0) {
		System.out.println("Clicked10");

		this._actionController.boutonHT();
	}	
}

