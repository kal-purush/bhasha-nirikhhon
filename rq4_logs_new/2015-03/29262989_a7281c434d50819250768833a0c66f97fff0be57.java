package javagame;

import org.newdawn.slick.*;
import org.newdawn.slick.state.BasicGameState;
import org.newdawn.slick.state.StateBasedGame;
import org.newdawn.slick.tiled.TiledMap;


public class LootMenu extends BasicGameState {

    // This game state
    private static int gameState;

    // singelton for player
    private Player player;

    //inventory object
    private Inventory enemyInventory;

    // Background
    private TiledMap lootMap;

    //buttons
    private Image addItemButton, addItemButtonPressed;
    private Image removeItemButton, removeItemButtonPressed;
    private Image addGold, goldAdded;
    private boolean addIsPressed, removeIsPressed, addGoldPressed;


    // Where the mouse is at
    private int mouseX, mouseY;

    private Items displayItem;

    private boolean inventoryItemSelected;

    private boolean settingName;

    private int[] inventoryItems, npcInventoryItems;

    private int selectedX, selectedY;


    public LootMenu( int gameState ) {
        this.gameState = gameState;
    }

    public void enter( GameContainer gc, StateBasedGame sbg ) {

        this.enemyInventory = new Inventory(this.player.getLootingInventory(), false);
    }

    public void init( GameContainer gc, StateBasedGame sbg ) throws SlickException {

        this.player = this.player.getInstance();


        this.inventoryItems = new int[ 21 ];
        this.npcInventoryItems = new int[ 21 ];

        // Inventory Background
        this.lootMap = new TiledMap( "NewEra-Beta/res/map/LootMenu.tmx" );

        addItemButton = new Image( "NewEra-Beta/res/buttons/up_white.png" );
        addItemButtonPressed = new Image( "NewEra-Beta/res/buttons/up_orange.png" );
        removeItemButton = new Image( "NewEra-Beta/res/buttons/down_white.png" );
        removeItemButtonPressed= new Image( "NewEra-Beta/res/buttons/down_orange.png" );

        addGold = new Image( "NewEra-Beta/res/buttons/plus_orange.png" );
        goldAdded = new Image( "NewEra-Beta/res/buttons/plus_white.png" );

        addIsPressed = false;
        removeIsPressed = false;
        addGoldPressed = false;


        this.settingName = false;
        this.inventoryItemSelected = false;



    }

    public void render( GameContainer gc, StateBasedGame sbg, Graphics g ) throws SlickException {

        this.lootMap.render( 0,0 );

        if(this.enemyInventory.getMoney() > 0) {
            addGoldPressed = false;
        } else {
            addGoldPressed = true;
        }

        this.player.getInventory().renderPlayerInventoryInLootMenu(g, this.inventoryItems);
        this.enemyInventory.renderNPCInventoryInLootMenu( g, this.npcInventoryItems );

        g.setColor(Color.black);
        if(player.getPlayerName() == null || player.getPlayerName() == "") {
            g.drawString("Player", 272, 40);
        } else {
            g.drawString(this.player.getPlayerName(), 272, 40);
        }

        g.drawString("Enemy", 272, 585);

        g.drawString("Gold: "+ this.player.getInventory().getMoney(), 96, 40);
        g.drawString("Gold: "+ this.enemyInventory.getMoney(), 448, 585);



        if(!addIsPressed) {
            this.addItemButton.draw(128, 288);
        } else {
            this.addItemButtonPressed.draw(128, 288);
        }

        if(!removeIsPressed) {
            this.removeItemButton.draw(160, 288);
        } else {
            this.removeItemButtonPressed.draw(160, 288);
        }

        if(!addGoldPressed) {
            this.addGold.draw(413, 576);
        } else {
            this.goldAdded.draw(413, 576);
        }


        // If item is selected
        if( this.displayItem != null ) {
            // Name
            g.setColor( Color.lightGray );
            g.fillRoundRect(240, 264, 256, 24, 10);
            if( !this.settingName ) {
                g.setColor(this.displayItem.getItemRarityColorNoAlpha());
                g.drawString(this.displayItem.getName(), 240, 272);
            }
            g.setColor(Color.black );
            // Class
            int classID = this.displayItem.getClassID();
            if( classID == 0 ) {
                g.drawString("Class: Hunter", 240, 292);
            }
            else if( classID == 1 ) {
                g.drawString("Class: Warrior", 240, 292);
            }
            else if( classID == 2 ) {
                g.drawString("Class: Wizard", 240, 292);
            }
            else if( classID == 3 ) {
                g.drawString("Class: Rogue", 240, 292);
            }
            else if( classID == 4 ) {
                g.drawString("Class: Any", 240, 292);
            }

            g.drawString( "Attack Bonus:  " + this.displayItem.getAttackPower() , 240, 312);
            g.drawString( "Defence Bonus: " + this.displayItem.getDefencePower() , 240, 332);
            g.drawString( "Item Value: " + this.displayItem.getWorth() , 240, 352);
            // Add worth

            g.setColor(Color.white);
            g.drawRect( this.selectedX, this.selectedY, 32, 32 );
        }
    }

    public void update( GameContainer gc, StateBasedGame sbg, int delta ) throws SlickException {

        Input input = gc.getInput();

        // Mouse X & Y
        this.mouseX = input.getMouseX();
        this.mouseY = input.getMouseY();

        int xPos, yPos, npcXPos, npcYPos;


        boolean spotSelected = false;

        // 'I' will return to game
        if (input.isKeyPressed(Input.KEY_ESCAPE) || input.isKeyPressed(Input.KEY_E)) {
            input.clearKeyPressedRecord();
            sbg.enterState(1);
        }

        //adding gold
        if ((this.mouseX >= 413 && this.mouseX <= 445) && (this.mouseY >= 576 && this.mouseY <= 608)) {
            if (input.isMousePressed(Input.MOUSE_LEFT_BUTTON)) {
                if(this.enemyInventory.getMoney() > 0) {
                    int gold = this.enemyInventory.getMoney();
                    this.enemyInventory.subMoney(gold);
                    this.player.getInventory().addMoney(gold);
                    this.addGoldPressed = true;
                }

            }

        }

        //adding to player inventory
        if ((this.mouseX >= 128 && this.mouseX <= 160) && (this.mouseY >= 288 && this.mouseY <= 320)) {
            if (input.isMousePressed(Input.MOUSE_LEFT_BUTTON)) {
                if(addIsPressed) {
                    this.player.getInventory().addItem(this.displayItem);
                    this.enemyInventory.dropItem(this.displayItem.getID());
                    this.addIsPressed = false;
                    this.inventoryItemSelected = false;
                    this.displayItem = null;


                }
            }
        }

        //
        if ((this.mouseX >= 160 && this.mouseX <= 192) && (this.mouseY >= 288 && this.mouseY <= 320)) {
            if (input.isMousePressed(Input.MOUSE_LEFT_BUTTON)) {
                if(removeIsPressed) {
                    this.enemyInventory.addItem(this.displayItem);
                    this.player.getInventory().dropItem(this.displayItem.getID());
                    this.addIsPressed = false;
                    this.inventoryItemSelected = false;
                    this.displayItem = null;


                }
            }
        }


        if(this.mouseY < 272) {

            xPos = 112;
            yPos = 80;
            for (int x = 0; x < this.player.getInventory().getInventoryCount(); x++) {
                if ((this.mouseX >= xPos && this.mouseX <= xPos + 32) && (this.mouseY >= yPos && this.mouseY <= yPos + 32)) {
                    if (input.isMousePressed(Input.MOUSE_LEFT_BUTTON)) {
                        this.displayItem = this.player.getInventory().getItemByID(this.inventoryItems[x]);
                        System.out.println(this.displayItem.getName());
                        x = this.inventoryItems.length;
                        this.selectedX = xPos;
                        this.selectedY = yPos;
                        this.inventoryItemSelected = true;
                        this.removeIsPressed = true;
                        this.addIsPressed = false;
                        spotSelected = true;
                        break;
                    }
                }
                xPos += 64;
                if (xPos > 496) {
                    xPos = 112;
                    yPos += 64;
                }
            }
        } else {

            xPos = 112;
            yPos = 400;
            for (int x = 0; x < this.enemyInventory.getFullInventoryCount(); x++) {
                if ((this.mouseX >= xPos && this.mouseX <= xPos + 32) && (this.mouseY >= yPos && this.mouseY <= yPos + 32)) {
                    if (input.isMousePressed(Input.MOUSE_LEFT_BUTTON)) {
                        this.displayItem = this.enemyInventory.getItemByID(this.npcInventoryItems[x]);
                        System.out.println(this.displayItem.getName());
                        x = this.npcInventoryItems.length;
                        this.selectedX = xPos;
                        this.selectedY = yPos;
                        this.inventoryItemSelected = true;
                        this.addIsPressed = true;
                        this.removeIsPressed = false;
                        spotSelected = true;
                        break;
                    }
                }
                xPos += 64;
                if (xPos > 496) {
                    xPos = 112;
                    yPos += 64;
                }
            }
        }

        if( input.isMousePressed( Input.MOUSE_LEFT_BUTTON ) && !spotSelected ) {
            this.inventoryItemSelected = false;
            this.displayItem = null;
            this.addIsPressed = false;
            this.removeIsPressed = false;
        }
    }

    public int getID( ) {
        return this.gameState;
    }


}
