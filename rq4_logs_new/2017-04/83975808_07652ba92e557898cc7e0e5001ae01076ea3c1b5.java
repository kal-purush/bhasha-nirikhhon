/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gameoflife;

import javafx.css.PseudoClass;
import javafx.scene.control.Button;

/**
 *
 * @author Espen
 */
public class GameBoardTile {
   
   private static final PseudoClass LIVE_PSEUDO_CLASS = PseudoClass.getPseudoClass("live");
   
   private Button tile;
   public GameBoardTile(String tileId, int tileSize){
       tile = new Button();
       tile.setId(tileId);
       setTileSize(tileSize);
    }
   
    /*refreshes the view of a single cell*/
    protected void refreshTile(boolean cellState){
            if (cellState){
                tile.pseudoClassStateChanged(LIVE_PSEUDO_CLASS, true);
            }
            else{
                tile.pseudoClassStateChanged(LIVE_PSEUDO_CLASS, false);  
            }
    }
    
    private void setTileSize(int size){
        tile.setMinSize(size, size);
        tile.setMaxSize(size, size);
    }
    
    protected Button getTile(){
        return tile;
    }
    
}