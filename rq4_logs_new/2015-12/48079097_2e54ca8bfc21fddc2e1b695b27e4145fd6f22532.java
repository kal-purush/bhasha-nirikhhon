package gameWithAlex;
//colin wakefield
import java.awt.*;

import carboardForest.Item;
import carboardForest.StandardWhite;
import carboardForest.Tile;
public class ItemHandler
{
    Item[] items;
    final int  WIDTH;
    final int HEIGHT;
    TileMap map;
    MainCharacter boy;
    final int size = 32;
    public ItemHandler(int w , int h, TileMap m,MainCharacter b)
    {
        items = new Item[8];
        WIDTH = w;
        HEIGHT = h;
        map = m;
        boy = b;
    }
    
    public boolean add(Item I)
    {
        // addes a item to the list if the list is not full
        int x = 0 ; 
        //finds a sopt where not fill
        while(x < items.length && items[x] != null)
        {
            x++;
        }
        if(x < items.length)
        {
        items[x] = I;
        return true;
        }
        else
        return false;
    }
    
    public void draw(Graphics g )
    {
        // draws black grid
        g.setColor(Color.black);
        g.fillRect(0,HEIGHT-size,size*items.length,size);
        //draws items in the correct spot
        for(int x = 0; x < items.length; x++)
        {
            if(items[x] != null)
            items[x].Draw(g,0,x,0 ,HEIGHT-size , size);
        }
    }
    
    public void update()
    {
        //runs through the hold grid
        for(int row = 0; row < map.getHeight(); row++)
            {
                for(int col = 0; col < map.getWidth(); col++)
                {
                    
                    //checks for item collision with the players death box
                    if(boy.getDeathbox().intersects(map.getTile(row, col).getRectangle()) && map.getTile(row, col).getType().equals("Gitem"))
                    {
                        // sets the players has to true or fase if the add is not full
                       ((Item)map.getTile(row, col)).playerHas(this.add((Item)map.getTile(row, col)));
                       //sets tile if has changed
                       if(((Item)map.getTile(row, col)).getHas())
                       {
                       map.setTile(row,col,new StandardWhite());
                       }
                    } 
                }
            }
        
    }
    public Item[] getItems()
    {
        return items;
    }

}