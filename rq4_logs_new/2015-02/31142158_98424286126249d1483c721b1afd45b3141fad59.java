import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
/**
 * The Game Model/Controller. This class creates the GUI by accessing 
 * the GameModel class and the Player Class.
 * 
 * @author Patrick (pjp5228) 
 *
 */
@SuppressWarnings({ "static-access", "unchecked", "rawtypes", "unused"})
public class Cellar {
    //The value that will contain the model
    private static Model model;
    
    //Lists that contain the info to create the cellar
    private static DefaultListModel<Trap> trapList;
    private static ArrayList<Room> roomList;
    private static DefaultListModel<Hall> hallList;
    //The Player Object
    private static Player p = new Player();
    //The Monster Object
    private static Monster m;
    //Lists that store the items that the player has and the room has.
    private static JList itemsInInventory;
    private static JList itemsInRoom;
    //The Swing elements to be added to the frame.
    private static JList nav;
    private static JTextArea prompt;
    private static JLabel hud;
    private static JButton moveIn;
    private static JButton moveOut;

    /**
     * Function that is called when the GUI needs to update
     */
    public static void update() {
        //Sets the Items in the inventory to the
        itemsInInventory.setModel(p.getInventory());
        itemsInRoom.setModel(roomList.get(p.getRoom()).getItemList());
        nav.setModel(model.getHallList(p.getRoom()));
        hud.setText("Health: " + p.getHealth() + "   "  + "Level: " + 
                p.getLevel());
        //Checks if the game is one. 
        //Disables all of the Swing components and prints a message
        if(p.wonGame()) {
            prompt.append("Congrats! You Won! \n");
            nav.setEnabled(false);
            itemsInRoom.setEnabled(false);
            itemsInInventory.setEnabled(false);
            moveIn.setEnabled(false);
            moveOut.setEnabled(false);
        }
        //Checks if the Player is dead. 
        //Disables all of the Swing components and prints a message
        if(p.isDead()) {
            prompt.append("The Player has died. Game Over. \n");
            nav.setEnabled(false);
            itemsInRoom.setEnabled(false);
            itemsInInventory.setEnabled(false);
            moveIn.setEnabled(false);
            moveOut.setEnabled(false);
        }
    }

    /**
     * Class that creates the GUI. It is run in a separate thread.
     */
    public static void createGUI() {
        //Create a JFrame with a size of 800 pixels by 600 pixels
        JFrame frame = new JFrame("The Cellar!! - Patrick Pistor(pjp5228)");
        frame.setVisible(true);
        frame.setDefaultCloseOperation(frame.EXIT_ON_CLOSE); 
        frame.setSize(new Dimension(800,600));
        //The master panel which contains all of the SWING components
        JPanel main = new JPanel();
        frame.add(main);
        //Sets the main panel to a border layout
        main.setLayout(new BorderLayout());
        //Declares the HUD which displays the current 
        //health and level of the player.
        hud = new JLabel("Health: " + p.getHealth() + "  "
                + " "  + "Level: " + p.getLevel());
        hud.setFont(new Font("Verdana", Font.PLAIN, 30));
        //Creates the JPanel that is to the Right
        JPanel sub1 = new JPanel(new GridLayout(0, 2, 4, 10));
        //Generates the list of halls and adds the move ListSelectionListener
        nav = new JList(hallList);
        nav.addListSelectionListener(move);
        JScrollPane scrollNav = new JScrollPane(nav);
        //Declares the JTextArea where the events are displayed
        prompt = new JTextArea();
        prompt.setEditable(false);
        JScrollPane scrollPrompt = new JScrollPane(prompt);
        //Adds all of the Components to the right JPanel
        sub1.add(scrollNav);
        sub1.add(scrollPrompt);
        //Creates the JPanel on the left
        JPanel sub2 = new JPanel(new BorderLayout(5, 5));
        //A sub-panel of it with the Item-lists and the buttons
        JPanel sub2_1 = new JPanel(new GridLayout(3, 0, 2, 2));
        //A sub-panel of that with the buttons
        JPanel sub2_1_1 = new JPanel(new GridLayout(2, 0));
        //A label that shows you where the player inventory is
        JLabel inven = new JLabel("Player Inventory");
        //Creates a ScrollPane with the players inventory
        JScrollPane scrollInven = new JScrollPane(itemsInInventory);
        //Declares the two buttons, and sets their text to Up and Down arrows
        moveIn = new JButton("\u2191");
        moveOut = new JButton("\u2193");
        //Add ActionListeners that make the add and remove functions
        moveIn.addActionListener(add);
        moveOut.addActionListener(remove);
        //Creates a JScrollPane with the itemsInRoom
        JScrollPane scrollRoom = new JScrollPane(itemsInRoom);
        //A label that shows you where the room item-list is
        JLabel roomItems = new JLabel("Items in Room");
        //Adds all of the objects to their JPanels
        sub2.add(inven, BorderLayout.NORTH);
        sub2_1.add(scrollInven);
        sub2_1_1.add(moveIn);
        sub2_1_1.add(moveOut);
        sub2_1.add(sub2_1_1);
        sub2_1.add(scrollRoom);
        sub2.add(sub2_1, BorderLayout.CENTER);
        sub2.add(roomItems, BorderLayout.SOUTH);
        //Sets the lists to single selection to avoid selection errors
        itemsInInventory.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        itemsInRoom.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, sub2,
                sub1);
        //Gets rid of the dividers on the JSplitPane
        split.setDividerSize(0);
        //Adds all of the Panels and JSplitPanes to the main panel
        main.add(hud,BorderLayout.NORTH);
        main.add(split,BorderLayout.CENTER);
        //Sets the JFrames Location to the center of the screen
        frame.setLocationRelativeTo(null);
    }
    /**
     * ActionListener that Allows the player to add something from
     * the room to his/her inventory.
     */
    private static ActionListener add = new ActionListener() {
        public void actionPerformed(ActionEvent e) {
            //Gets the selected item.
            Item selectedItem = (Item)itemsInRoom.getSelectedValue();
            if(selectedItem == null) {
                //Does nothing if there is nothing selected
                prompt.append("Nothing is selected! \n");
            }
            else {
                //If it is the sword or the Amulet, add to the inventory, 
                //but don't count as protection
                if(selectedItem.toString().trim().equals("Sword") || 
                        selectedItem.toString().trim().equals("Amulet")){
                    //Remove the selected Item from the roomList
                    roomList.get(p.getRoom()).removeItem(selectedItem);
                    //Add the selected item to the Players inventory
                    p.addToInventory(selectedItem);
                    //Send the message to the event window
                    prompt.append("Player added " + selectedItem.toString() + 
                            " to inventory. \n");
                    //Let the monster take a move
                    prompt.append(p.takeAction(m));
                }
                else {
                    if(p.getInventorySize() < p.getLevel()) {
                        //Remove the selected Item from the roomList
                        roomList.get(p.getRoom()).removeItem(selectedItem);
                        //Add the selected item to the Players inventory
                        p.addToInventory(selectedItem);
                        //Send the message to the event window
                        prompt.append("Player added " + selectedItem.toString() 
                                + " to inventory. \n");
                        //Let the monster take a move
                        prompt.append(p.takeAction(m));
                    }
                    else {
                        //If there isn't enough room in the inventory print 
                        //this message
                        prompt.append("Not enough inventory room! \n");
                    }
                }
                //Call the update function
                update();

            }
        }
    };
    /**
     * The ActionListener that controls the remove button
     * 
     */
    private static ActionListener remove = new ActionListener() {
        public void actionPerformed(ActionEvent e) {
            //The currently selected item
            Item selectedItem = (Item)itemsInInventory.getSelectedValue();
            if(selectedItem == null) {
                //If nothing is selected, print this message.
                prompt.append("Nothing is selected! \n");
            }
            else {
                //Add the item to the room Item-list.
                roomList.get(p.getRoom()).addItem(selectedItem);
                //Remove the item from the player inventory
                p.removeFromInventory(selectedItem);
                //Print the message
                prompt.append("Player removed " + selectedItem.toString() 
                        + " from inventory. \n");
                //Call the update function
                update();
                prompt.append(p.takeAction(m));
            }

        }
    };
    /**
     * The ListSelectionListener that handles the movement from 
     * room to room.
     */
    private static ListSelectionListener move = new ListSelectionListener() {
        public void valueChanged(ListSelectionEvent e) {
            //Gets the selected hall
            Hall selectedHall = (Hall)nav.getSelectedValue();
            //Boolean that checks if the the list is adjusting
            boolean isAdjusting = e.getValueIsAdjusting();
            
            if(nav.getSelectedIndex() >= 0) { 
                //If the selectedIndex > 0
                if(!isAdjusting){
                    //If the list isn't adjusting
                    if(p.getRoom() == selectedHall.getFinish()) {
                        //If the current room is equal to the selected halls 
                        //finish
                        //Move to the start of the hall
                        p.moveToRoom(selectedHall.getStart());
                        //If there is a trap, something happens
                        prompt.append(roomList.get(selectedHall.getStart())
                                .enteredRoom(p));
                        //If there is a monster in the room, something happens
                        prompt.append(p.takeAction(m));
                    }
                    else {
                        //If the current room is equal to the selected halls 
                        //start
                        p.moveToRoom(selectedHall.getFinish());
                        //If there is a trap, something happens
                        prompt.append(roomList.get(selectedHall.getFinish())
                                .enteredRoom(p));
                        //If there is a monster in the room, something happens
                        prompt.append(p.takeAction(m));
                    }
                    //Call the update function
                    update();
                }
            }
        }
    };
    /**
     * Takes one argument, a file, creates a new Model, and then
     * creates the GUI and Game-Loop.
     * 
     * @param args
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
        //Gets the file, checks it, and sends it to the model
        File f = null;
        String filePath;
        if(args.length != 1) {
            System.err.println("Usage: java Cellar config_file_name");
            System.exit(1);
        }
        filePath = args[0];
        URL path = ClassLoader.getSystemResource(filePath);
        try {
            f = new File(path.toURI());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        model = new Model(f);
        //Declares instance variables based off of the model. 
        roomList = model.getRoomList();
        hallList = model.getHallList(p.getRoom());
        itemsInRoom = new JList(roomList.get(p.getRoom()).getItemList());
        itemsInInventory = new JList(p.getInventory());
        m = model.getTheMonster();
        if(m != null)
            m.setGameModel(model);
        //Invokes the GUI thread
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createGUI();
            }
        });
    }
}