/**
 * Created by Ian James Fannon on 1/11/2017.
 */
public class Item {

    private String itemName;
    private int itemPrice;

    public Item() {
        itemName = "";
        itemPrice = 0;
    }

    /**
     * The method toString prints the items in a special format
     * @param itemName is the name of the item
     * @param itemPrice is the price of the item
     * @return the item with the name and price concatenated together
     */
    public String toString(String itemName, int itemPrice) {
        this.itemName = itemName;
        this.itemPrice = itemPrice;
        String item = itemName + ":   " + itemPrice;
        return item;
    }

    // Setters and Getters
    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public int getItemPrice() {
        return itemPrice;
    }

    public void setItemPrice(int itemPrice) {
        this.itemPrice = itemPrice;
    }
}