using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Bartok : MonoBehaviour {
	static public Bartok S; 
	public TextAsset deckXML; 
	public TextAsset layoutXML; 
	public Vector3 layoutCenter = Vector3. zero; 
	public float handFanDegrees = 10f;
	public bool ________________; 
	public Deck deck; 
	public List < CardBartok > drawPile;
	public List < CardBartok > discardPile; 
	
	public BartokLayout layout;
	public Transform layoutAnchor;
	
	public List<Player> players;
	public CardBartok targetCard;
	
	
	void Awake() { 
		S = this; 
	} 
	void Start () { 
		deck = GetComponent < Deck >(); // Get the Deck 
		deck.InitDeck( deckXML.text); // Pass DeckXML to it 
		Deck.Shuffle( ref deck.cards); // This shuffles the deck 
		// The ref keyword passes a reference to deck.cards, which allows 
		// deck.cards to be modified by Deck.Shuffle()
		layout = GetComponent < BartokLayout >(); // Get the Layout 
		layout.ReadLayout( layoutXML.text); // Pass LayoutXML to it 
		drawPile = UpgradeCardsList( deck.cards );
		LayoutGame ();
		
		
	}
	// UpgradeCardsList casts the Cards in lCD to be CardBartoks 
	// Of course, they were all along, but this lets Unity know it 
	List < CardBartok > UpgradeCardsList( List < Card > lCD) { 
		List < CardBartok > lCB = new List < CardBartok >(); 
		foreach( Card tCD in lCD ) { 
			lCB.Add ( tCD as CardBartok ); 
		} 
		return( lCB ); 
	}
	// Position all the cards in the drawPile properly 
	public void ArrangeDrawPile() { 
		CardBartok tCB; 
		for (int i = 0; i < drawPile.Count; i ++) { 
			tCB = drawPile[ i]; 
			tCB.transform.parent = layoutAnchor; 
			tCB.transform.localPosition = layout.drawPile.pos;
			// Rotation should start at 0 
			tCB.faceUp = false; 
			tCB.SetSortingLayerName( layout.drawPile.layerName); tCB.SetSortOrder(-i* 4); // Order them front-to-back 
			tCB.state = CBState.drawpile; 
		} 
	} // Perform the initial game layout 
	void LayoutGame() { 
		// Create an empty GameObject to serve as an anchor for the tableau 
		if (layoutAnchor == null) { GameObject tGO = new GameObject("_LayoutAnchor"); // ^ Create an empty GameObject named _LayoutAnchor in the Hierarchy 
			layoutAnchor = tGO.transform; // Grab its Transform 
			layoutAnchor.transform.position = layoutCenter; // Position it 
		} // Position the drawPile cards 
		ArrangeDrawPile(); // Set up the players 
		Player pl; 
		players = new List < Player >(); 
		foreach (SlotDef tSD in layout.slotDefs) { 
			pl = new Player();
			pl.handSlotDef = tSD; players.Add( pl); 
			pl.playerNum = players.Count; 
		} 
		players[0]. type = PlayerType.human; // Make the 0th player human 
	} 
	// The Draw function will pull a single card from the drawPile and return it 
	public CardBartok Draw() { 
		CardBartok cd = drawPile[ 0]; // Pull the 0th CardProspector 
		drawPile.RemoveAt( 0); // Then remove it from List < > drawPile 
		return( cd); // And return it 
	} // This Update method is used to test adding cards to players' hands 
	
	void Update() { 
		if (Input.GetKeyDown (KeyCode.Alpha1)) { 
			players [0].AddCard (Draw ()); 
		} 
		if (Input.GetKeyDown (KeyCode.Alpha2)) { 
			players [1].AddCard (Draw ()); 
		}
		if (Input.GetKeyDown (KeyCode.Alpha3)) { 
			players [2].AddCard (Draw ()); 
		} 
		if (Input.GetKeyDown (KeyCode.Alpha4)) { 
			players [3].AddCard (Draw ());
		}
	}
	
	
	
	
	
	
	
}