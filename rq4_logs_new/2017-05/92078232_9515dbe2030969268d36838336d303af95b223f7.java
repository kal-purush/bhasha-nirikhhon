package org.jfclarkjr.java3hw2;

/**
 * InventoryApp is a class containing the main method for accessing an 
 * inventory system composed of DVDs, CDs, and Books
 * 
 * @author John Clark
 * @since 1.8
 *
 */
public class InventoryApp
{
	public static void main(String[] args)
	{
		InventoryModel model = new InventoryModel();
		InventoryController controller= new InventoryController(model);
		InventoryView view = new InventoryView(controller);
		
		// Register the view and model objects with each other
		model.setView(view);
		view.setModel(model);
		
		// Start the interactive user interface
		view.start();
	}
}