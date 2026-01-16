package game;

public class PopupTile extends Tile{
    Popup popup;
    public PopupTile(String tileImage, boolean isWalkable, Popup popup){
        super(tileImage, isWalkable);
        popup.isVisible=false;
        this.popup=popup;
    }
    public void enablePopup(){
        this.popup.isVisible=true;
    }
    public void disablePopup(){
        this.popup.isVisible=false;
    }
}