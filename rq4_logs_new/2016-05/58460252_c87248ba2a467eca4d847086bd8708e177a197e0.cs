using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class TextController : MonoBehaviour {

	public Text text;
	private enum States {cell_start, cell, corridor1, corridor2, corridor3, corridor4, doorlock, cockpit, help, credits };
	private States myState;
	private States SaveState;
	
	// Flags, prefix flg_
	private bool flg_ghosting = true, flg_first_unghosting = false;
	private bool flg_cell_visited = false;
	private bool flg_corr1_visited = false;
	private bool flg_corr2_visited = false;
	private bool flg_corr3_visited = false;
	private bool flg_corr4_visited = false;
	private bool flg_extinguisher = false;
	private bool flg_trueLove = false;

	// Use this for initialization
	void Start () {
		myState = States.cell_start;
	
	}
	
	// Update is called once per frame
	void Update () {
		print (myState);
		switch(myState) {
			case States.cell_start: cell_start(); 	break;
			case States.cell: 		cell();		 	break;
			case States.corridor1: 	corridor1();	break;
			case States.corridor2:	corridor2();	break;
			case States.corridor3:	corridor3();	break;
			case States.corridor4:	corridor4();	break;
			case States.doorlock:	doorlock();		break;
			case States.cockpit:	cockpit();		break;
			case States.help:		help();			break;
			case States.credits:	credits();		break;
			default:
				text.text = "ERROR: Room does not exist.";
				break;
		}
		
		/*if (myState == States.cell_start) {
			;
		} else if (myState == States.corridor1) {
			corridor1();
		}*/
	}
	
	void cell_start () {
		/*if (flg_ghosting) { */
		text.text = 
			"The agony is intense, a cold white stab lancing your brain." + 
			"\n\tWhen you regain your senses, you find yourself standing in a dark, " +
			"cramped space. A cell? " +
			"\n\tTo your left, barely visible in the gloom, you see a chair, lying on its side. " +
			"Did you do that? You try to collect your thoughts, still reeling from the pain, but try " +
			"as you might you can’t remember how you got here. " +
			"\n\tA door to the WEST stands open, leading out into more darkness beyond." + 
			"\n\nUse the ASWD/Arrow Keys to move." +
			"\nPress A to head West, out the door." +
			"\n\nPress H for Help.";
		/*} else if(!ghosting) {
			// We're never in this state
		}*/
		
		if(key_controller() == 1) {
			myState = States.corridor1;
		} else if(key_controller() == 6) {
			SaveState = States.cell_start;
			myState = States.help;
		}
	}
	
	void cell () {
		/* 	A little tricky:
		*	flg_cell_visited triggers when you first step back into the cell
		*	flg_first_unghosting triggers when you touch her arm
		*/
		if ((!flg_cell_visited) && (flg_ghosting)) { 
			text.text = "You step back into the cell. You freeze." +
				"\n\tShrouded in roiling blue flames, a woman stands before you, her eyes " +
				"closed and her head forward, as if in prayer. She doesn’t seem to notice the flames " +
				"or you." +
				"\n\tShe has long, dark hair that hangs down to her waist, and wears a white " +
				"uniform with black indication markings. A name badge over her left breast reads " +
				"‘Inmate 13’, with the words ‘Projection Lock’ written beneath." +
				"\nPress “SPACE” to reach out and touch her arm.";
			//Change pic of woman
		} else if((!flg_first_unghosting)/* && (!ghosting)*/) {
			text.text = "For the first time you realize how bitterly cold it is in the cell. Then you realize that " +
		 				"the woman has disappeared. You whirl around, but she’s nowhere to be seen. Then " +
		 				"you realize that you can feel more than just the numbing cold. You feel heavy hair" +
		 				" whipping about as you turn. Lifting your hands to your face, you see the slender " +
		 				"fingers in white gloves. "+
		 				"\n\tYou are in her body.";
		} else if (/*(flg_cell_visited) && */(flg_ghosting)) {
			text.text = "ghosting";
		} else if (!flg_ghosting) {
			text.text = "unghosted";
		}
		
		if(key_controller() == 1) {
			// 
			if(flg_cell_visited) { 
				flg_first_unghosting = true;
				if(flg_ghosting)
					flg_ghosting = false;
				else
					flg_ghosting = true;
			}
			myState = States.corridor1;
		} else if(key_controller() == 5) {
			flg_cell_visited = true;
			myState = States.cell;
		} else if(key_controller() == 6) {
			SaveState = States.cell;
			myState = States.help;
		}
	}
	
	void corridor1 () {
		if (!flg_corr1_visited) {
			text.text = "You step out into a corridor lit by faint, ghostly luminescent " +
				"emergency strips, disappearing off into the darkness to the NORTH. To " +
				"the EAST is the room from which you came, while a heavy security door " +
				"bars the way WEST. Near you, the southern wall has a dead data screen " +
				"and ‘Cell Block 6’ stencilled on the rusted metal wall.";
		} else if (flg_corr1_visited) {
			text.text = "You are at the southern end of a corridor lit by faint, ghostly " +
				"luminescent emergency strips which disappear off into the darkness to the" + 
				" NORTH. To the EAST is the room you started in, while a heavy security " +
				"door stands to the WEST. Near you, the southern wall has a dead data " +
				"screen and ‘Cell Block 6’ stencilled on the rusted metal wall. ";
		}
		
		if(key_controller() == 1) {
			//myState = States.corridor1;
		} else if(key_controller() == 2) {
			flg_corr1_visited = true; //Once we visit it is always true, no need to check.
			myState = States.corridor2;
		} else if(key_controller() == 4) {
			flg_corr1_visited = false; //Once we visit it is always true.
			myState = States.cell; //This changes to turn human
		} else if(key_controller() == 6) {
			SaveState = States.corridor1;
			myState = States.help;
		}
	}
	
	void corridor2 () {
		text.text = "More darkness?\n\n" +
			"The CORRIDOR continues NORTH and SOUTH. More cells lie to the EAST and WEST";
	
		if(key_controller() == 1) {
			//myState = States.cell_start;
		} else if(key_controller() == 2) {
			myState = States.corridor3;
		} else if(key_controller() == 3) {
			myState = States.corridor1;
		} else if(key_controller() == 4) {
			//myState = States.corridor1;
		} else if(key_controller() == 6) {
			SaveState = States.corridor2;
			myState = States.help;
		}
	}
	
	void corridor3 () {
		flg_ghosting = false;
		text.text = "You can see a door at the end of the corridor?\n\n" +
			"The CORRIDOR continues NORTH and SOUTH. More cells are situated to the EAST and WEST";
		if(key_controller() == 1) {
			//myState = States.cell_start;
		} else if(key_controller() == 2) {
			myState = States.corridor4;
		} else if(key_controller() == 3) {
			myState = States.corridor2;
		} else if(key_controller() == 6) {
			SaveState = States.corridor3;
			myState = States.help;
		}
	}
	
	void corridor4 () {
		if (flg_ghosting) { 
			text.text = "corridor 4 flg_ghosting";
		} else if(!flg_ghosting) {
			text.text = "corridor 4. Not flg_ghosting.";
		}
		
		if(key_controller() == 1) {
			//myState = States.corridor1;
		} else if(key_controller() == 2) {
			myState = States.doorlock;
		} else if(key_controller() == 3) {
			myState = States.corridor3;
		} else if(key_controller() == 4) {
			//myState = States.corridor2;
		} else if(key_controller() == 6) {
			SaveState = States.corridor4;
			myState = States.help;
		}
	}
	
	void doorlock () {
		if (flg_ghosting) { 
			text.text = "Doorlock ghosting";
		} else if(!flg_ghosting) {
			text.text = "Doorlock. Not ghosting.";
		}
		
		if(key_controller() == 2) {
			myState = States.cockpit;
		} else if(key_controller() == 3) {
			myState = States.corridor4;
		} else if(key_controller() == 6) {
			SaveState = States.doorlock;
			myState = States.help;
		}
	}
	
	void cockpit () {
		if (flg_ghosting) { 
			text.text = "Ships bridge, ghosting";
		} else if(!flg_ghosting) {
			text.text = "Ships bridge, not ghosting.";
		}
		
		text.text = "\n\nPress SPACE to continue";
		
		if(key_controller() == 5) {
			myState = States.credits; //change to see credits
		} else if(key_controller() == 6) {
			SaveState = States.doorlock;
			myState = States.help;
		}
	}
	
	void help () {
		text.text = "+++ How To Play +++" +
			"\n\nA or Right Arrow: Move West." +
			"\nW or Up Arrow: Move North." +
			"\nS or Down Arrow: Move South." +
			"\nD or Right Arrow: Move East." +
			"\nSPACE Key: Interact with an object or character." +
			"\nH: This help menu." +
			"\n\nPress H to return.";
		if(key_controller() == 6)
			myState = SaveState;
	}
	
	void credits () {
		text.text = "CREDITS" +
			"\n\nGAME\t\t\t\trodney sloan" +
			"\n\nMUSIC\t\t\t\tarctic caves" +
			"\n\t\t\t\t\t\ttechnoaxe" +
			"\n\nPUBLISHER\t\trising phoenix games" +
			"\n\t\t\t\t\t\twww.risingphoenixgames.com" +
			"\n\nThanks for playing..." +
			"\n\nPress SPACE to replay.";
		if(key_controller() == 5) {
			myState = States.cell_start; //change to see credits
		} else if(key_controller() == 6) {
			SaveState = States.credits;
			myState = States.help;
		}
	}
	
	/* Maps integers to common keys
	 * Returns int
	 */	
	int key_controller () {
	
		if((Input.GetKeyDown(KeyCode.A)) || (Input.GetKeyDown(KeyCode.LeftArrow))) {
			return 1;
		} else if((Input.GetKeyDown(KeyCode.W)) || (Input.GetKeyDown(KeyCode.UpArrow))) {
			return 2;
		} else if((Input.GetKeyDown(KeyCode.S)) || (Input.GetKeyDown(KeyCode.DownArrow))) {
			return 3;
		} else if((Input.GetKeyDown(KeyCode.D)) || (Input.GetKeyDown(KeyCode.RightArrow))) {
			return 4;
		} else if (Input.GetKeyDown(KeyCode.Space)) {
			return 5;
		} else if (Input.GetKeyDown(KeyCode.H)) { //H key for Help
			return 6;
		} 
		else 
			return 0;
			
		/*switch (Input.GetKeyDown(KeyCode)) {
			case KeyCode.D:
			case KeyCode.RightArrow:
				return 1;
				break;
			case KeyCode.W:
			case KeyCode.LeftArrow:
				return 2;
				break;
			default:
				return 0;
				break;
		}*/
	}

}