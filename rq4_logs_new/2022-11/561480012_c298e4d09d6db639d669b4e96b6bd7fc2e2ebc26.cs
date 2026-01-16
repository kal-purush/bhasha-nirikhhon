using System.Collections;
using System.Collections.Generic;
using UnityEngine;
namespace GolfSolitaire{

public enum eCardState {
drawpile,
tableau,
target,
discard
}

public class CardGolf : Card {

[Header("Set Dynamically: CardGolf")]
// This is how you use the enum eCardState
public eCardState state = eCardState.drawpile;
// The hiddenBy list stores which other cards will keep this one face down

public List<CardGolf> hiddenBy = new List<CardGolf>();
// The layoutID matches this card to the tableau XML if it's a tableau card

public int layoutID;
// The SlotDef class stores information pulled in from the LayoutXML <slot>

public SlotDef slotDef;
    // Start is called before the first frame update
    
    override public void OnMouseUpAsButton() {
    // Call the CardClicked method on the Prospector singleton

    Golf.S.CardClicked(this);
    // Also call the base class (Card.cs) version of this method
    base.OnMouseUpAsButton(); // a
}
}
}