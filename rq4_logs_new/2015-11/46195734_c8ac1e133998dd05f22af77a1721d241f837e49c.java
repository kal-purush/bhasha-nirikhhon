import java.awt.*;
import javax.swing.*;

public class Screen02Applet extends JApplet
{
    //Temp array for testing JLists
    private String[] testListItems = 
        { "Item #1 - $4.99", "Item #2 - $4.99", "Item #3 - $4.99", "Item #4 - $4.99" };
    private String[] testMenuItems = 
        { "Menu #1", "Menu #2", "Menu #3", "Menu #4" };
    
    //Two lists for selecting food and for listing the current order
    private JList foodMenuList, orderList;
    private JScrollPane foodScrollPane, orderScrollPane;
    
    //Panel for formatting the first row's middle section
    private JPanel middleSectionContainer;
    private JPanel logoPlaceholder;
    private JPanel subtotalContainer;
    private JComboBox menuSelector;
    
    //Buttons for the final row for ordering and checkout
    private JButton addToOrderButton, checkoutButton, removeFromOrderButton;
    
    public Screen02Applet()
    {
        //Screen 2 Prototype
        
        //Set the screen to have a GridLayout with 1 row and 3 columns.
        this.setLayout(new GridLayout(2, 3, 2, 2));
        
        foodMenuList = new JList(testListItems);
        orderList = new JList(testListItems);
        
        foodScrollPane = new JScrollPane(foodMenuList);
        orderScrollPane = new JScrollPane(orderList);
        
        //Limit the user from selecting more than one item per list
        foodMenuList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        orderList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        
        /* Middle Section */
        middleSectionContainer = new JPanel(new GridLayout(3, 1, 0, 2));
        
        logoPlaceholder = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 5));
        logoPlaceholder.add(new JLabel("Add Logo Here?"));
        
        menuSelector = new JComboBox(testMenuItems);
        subtotalContainer = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 5));
        
        JLabel testLabel = new JLabel("Subtotal: $0.00");
        subtotalContainer.add(testLabel);
        
        middleSectionContainer.add(logoPlaceholder);
        middleSectionContainer.add(subtotalContainer);
        middleSectionContainer.add(menuSelector);
        /* Middle Section */
        
        addToOrderButton = new JButton("Add to Order");
        checkoutButton = new JButton("Checkout");
        removeFromOrderButton = new JButton("Remove from Order");
        
        //Add the UI elements to the JApplet
        this.add(foodScrollPane);
        this.add(middleSectionContainer);
        this.add(orderScrollPane);
        this.add(addToOrderButton);
        this.add(checkoutButton);
        this.add(removeFromOrderButton);
    }
}