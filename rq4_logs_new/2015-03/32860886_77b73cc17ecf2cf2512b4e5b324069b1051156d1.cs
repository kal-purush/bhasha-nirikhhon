//Project EcliPse - Shenkar final project 2015.
//Gal Shalit, Yaniv Levi, David Faizulaev & Avishag Zehavi
using UnityEngine;
using UnityEngine.UI;// we need this namespace in order to access UI elements within our script
using System.Collections;
 
public class UI_Script : MonoBehaviour 
{
	//Main menu elements
	public Canvas CanvasMain_Menu;
	public Button ButtonSINGLEPLAYER;
	public Button ButtonMULTIPLAYER;
	public Button ButtonQUIT;

	//Sub menu elements
	public Canvas CanvasSub_Menu;
	public Button ButtonPILOT;
	public Button ButtonGUNNER;
	public Button ButtonBACK;

    void Start ()
    {
		Debug.Log ("Load main menu");

		//Initialize Main menu elements
		CanvasMain_Menu = CanvasMain_Menu.GetComponent<Canvas>();
		ButtonSINGLEPLAYER = ButtonSINGLEPLAYER.GetComponent<Button> ();
		ButtonMULTIPLAYER = ButtonMULTIPLAYER.GetComponent<Button> ();
		ButtonQUIT = ButtonQUIT.GetComponent<Button>();

		//Initialize Sub menu elements
		CanvasSub_Menu = CanvasSub_Menu.GetComponent<Canvas>();
		CanvasSub_Menu.enabled = false;
		ButtonPILOT = ButtonPILOT.GetComponent<Button> ();
		ButtonGUNNER = ButtonGUNNER.GetComponent<Button> ();
		ButtonBACK = ButtonBACK.GetComponent<Button> ();

		Debug.Log ("Main menu loaded");
    }
 
    public void SinglePlayerPress() //this function will be used on our Exit button
    {
		Debug.Log ("SinglePlayerPress");
		CanvasMain_Menu.enabled = false; //disable the main menu
		CanvasSub_Menu.enabled = true; //enable the Select Role menu

		//Enable sub menu buttons.
		ButtonPILOT.enabled = true;
		ButtonGUNNER.enabled = true;
		ButtonBACK.enabled = true;
    }
 
	public void MultiPlayerPress() //this function will be used on our Exit button
	{
		Debug.Log ("MultiPlayerPress");
		CanvasMain_Menu.enabled = false; //disable the main menu
		CanvasSub_Menu.enabled = true; //enable the Select Role menu
		
		//Enable sub menu buttons.
		ButtonPILOT.enabled = true;
		ButtonGUNNER.enabled = true;
		ButtonBACK.enabled = true;

	}

	public void GunnerPress() //this function will be used on our Gunner button
	{
		Debug.Log ("GunnerPress");
		CanvasMain_Menu.enabled = false;
		CanvasSub_Menu.enabled = false;
		//After role is selected the NetworkManager object script is called and user is connected to Photon server
	}

	public void PilotPress() //this function will be used on our Pilot button
	{
		Debug.Log ("PilotPress");
		CanvasMain_Menu.enabled = false;
		CanvasSub_Menu.enabled = false;
		//After role is selected the NetworkManager object script is called and user is connected to Photon server
	}

	//Go back to main menu
	public void PressBack()
	{
		Debug.Log ("PressBack");
		//Disable sub menu
		CanvasSub_Menu.enabled = false;

		//Re-enable main menu
		CanvasMain_Menu.enabled = true;
	}
	
    public void StartLevel () //this function will be used on our Play button
    {
	 	//Application.LoadLevel (1); //this will load our first level from our build settings. "1" is the second scene in our game 
    }
 
    public void QuitGamePress () //This function will be used on our "Yes" button in our Quit menu       
    {
		Debug.Log ("QuitGamePress");
  		Application.Quit(); //this will quit our game. Note this will only work after building the game    
    }
}