package unitTests.Item;

import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;
public class Item {

    @Test
    public void addSingleItem() {
        com.game.item.Item item = new com.game.item.Item("Test Item","Item to test functionality of the Item Class", com.game.item.Item.ITEM_TYPE_DAMAGE,10,1);
        int expected = 2;
        try {
            item.add();
        } catch (com.game.item.Item.AddItemCapException e) {
            e.printStackTrace();
        }
        int actual = item.getCount();
        assertEquals(expected, actual);
    }

    @Test
    public void addMultipleItems() {
        com.game.item.Item item = new com.game.item.Item("Test Item","Item to test functionality of the Item Class", com.game.item.Item.ITEM_TYPE_DAMAGE,10,1);
        int expected = 99;
        try {
            item.add(98);
        } catch (com.game.item.Item.AddItemCapException e) {

        }
        int actual = item.getCount();
        assertEquals(expected, actual);
    }

    @Test(expected = com.game.item.Item.AddItemCapException.class)
    public void itemCapHandler() throws com.game.item.Item.AddItemCapException {
        com.game.item.Item item = new com.game.item.Item("Test Item","Item to test functionality of the Item Class", com.game.item.Item.ITEM_TYPE_DAMAGE,10,1);
        item.add(100);
    }
}