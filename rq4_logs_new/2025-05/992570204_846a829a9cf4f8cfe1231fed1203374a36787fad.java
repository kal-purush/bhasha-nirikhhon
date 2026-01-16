package studio.jawa.bullettrain.components.gameplay;

import com.badlogic.ashley.core.Component;

public class InteractionComponent implements Component {
    public boolean canInteract = true;
    public String promptMessage = "";
    public InteractionType interactionType;
    public float interactionRadius = 50f; // Distance untuk show prompt
    
    public InteractionComponent() {}
    
    public InteractionComponent(InteractionType type, String message) {
        this.interactionType = type;
        this.promptMessage = message;
    }
    
    public enum InteractionType {
        DOOR_TRANSITION,
        PICKUP_ITEM,
    }
}