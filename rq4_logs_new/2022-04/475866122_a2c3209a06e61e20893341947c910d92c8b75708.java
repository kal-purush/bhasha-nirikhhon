package rebuild;

public class ActionCard {
    private CardType type;
    private int magnitude;
    private boolean movement = false;
    
    ActionCard() {
	init();
	cardModifiers();
    }
    
    public ActionCard(CardType cardType) {
	type = cardType;
	cardModifiers();
    }

    private void init() {
	double val = Math.random();
	
	if(val < 0.225) {
	    setCardType(CardType.Move1);
	} else if(val < 0.425) {
	    type = CardType.Move2;
	} else if(val < 0.55) {
	    type = CardType.Move3;
	} else if(val < 0.75) {
	    type = CardType.TurnL;
	} else if(val < 0.95) {
	    type = CardType.TurnR;
	} else {
	    type = CardType.UTurn;
	}
    }
    
    public void cardModifiers() {
	if(type.equals(CardType.Move1)) {
	    movement = true;
	    magnitude = 1;
	} else if(type.equals(CardType.Move2)) {
	    movement = true;
	    magnitude = 2;
	} else if(type.equals(CardType.Move3)) {
	    movement = true;
	    magnitude = 3;
	} 
    }

    public CardType getCardType() {
	return type;
    }

    public void setCardType(CardType type) {
	this.type = type;
    }

    public boolean isMovement() {
	return movement;
    }
    
    public int getMagnitude() {
	return magnitude;
    }

}