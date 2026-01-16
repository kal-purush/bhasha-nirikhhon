package clients.productlist;

import middle.MiddleFactory;
import middle.Names;
import middle.RemoteMiddleFactory;

import javax.swing.*;

/**
 * The standalone Products List Client
 */
public class ProductsListClient {
    public static void main(String[] args) {
        String stockURL = args.length < 1         // URL of stock R
                ? Names.STOCK_R                   // default location
                : args[0];                        // supplied location

        RemoteMiddleFactory mrf = new RemoteMiddleFactory();
        mrf.setStockRInfo(stockURL);
        displayGUI(mrf);                          // Create GUI
    }

    /**
     * Setup and display the UI for the Products List Client.
     *
     * @param mf MiddleFactory instance for database communication
     */
    private static void displayGUI(MiddleFactory mf) {
        JFrame window = new JFrame();
        window.setTitle("Products List Client");
        window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        ProductsListModel model = new ProductsListModel(mf);
        ProductsListView view = new ProductsListView(window, model, 0, 0);

        model.addPropertyChangeListener(view);    // Add property change listener to the model
        window.setVisible(true);                  // Display the window
        model.loadProducts();// Load the products list initially
    }
}