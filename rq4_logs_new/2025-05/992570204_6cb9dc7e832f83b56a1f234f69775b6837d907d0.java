package studio.jawa.bullettrain.components.level;

import com.badlogic.ashley.core.Component;
import com.badlogic.gdx.math.Rectangle;
import studio.jawa.bullettrain.data.ObjectType;

public class BaseObjectComponent implements Component {
    public ObjectType objectType;
    public Rectangle bounds = new Rectangle();
    public int carriageNumber;
    public boolean isDestructible = false; 
    
    public BaseObjectComponent() {}
    
    public BaseObjectComponent(ObjectType objectType, int carriageNumber) {
        this.objectType = objectType;
        this.carriageNumber = carriageNumber;
        
        // Set destructible for TNT
        this.isDestructible = (objectType == ObjectType.TNT);
    }
    
    public void setBounds(float x, float y) {
        bounds.set(x - objectType.width/2f, y - objectType.height/2f, 
                  objectType.width, objectType.height);
    }
}