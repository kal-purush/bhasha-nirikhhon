using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class InventoryScript : MonoBehaviour {

    public GameObject panel1;
    public GameObject panel2;
    public GameObject panel3;
    public GameObject panel4;
    public GameObject panel5;
    public GameObject playerObject;
    public Sprite pencil;
    public Sprite paper;
    public Sprite text;
    public Sprite ruler;
    public Sprite note;

    // Use this for initialization
    void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        for (int i = 1; i < 6; i++)
        {
            if (playerObject.GetComponent<PlayableCharacter>().inventory[i] != null)
            {
                Sprite s = null;
                if (playerObject.GetComponent<PlayableCharacter>().inventory[i] is IWeapon)
                {
                    if (((IPlayerWeapon)playerObject.GetComponent<PlayableCharacter>().inventory[i]).Name == "Pencil")
                    {
                        s = pencil;
                    }
                    if (((IPlayerWeapon)playerObject.GetComponent<PlayableCharacter>().inventory[i]).Name == "Paper")
                    {
                        s = paper;
                    }
                    if (((IPlayerWeapon)playerObject.GetComponent<PlayableCharacter>().inventory[i]).Name == "Textbook")
                    {
                        s = text;
                    }
                    if (((IPlayerWeapon)playerObject.GetComponent<PlayableCharacter>().inventory[i]).Name == "Notebook")
                    {
                        s = note;
                    }
                    if (((IPlayerWeapon)playerObject.GetComponent<PlayableCharacter>().inventory[i]).Name == "Ruler")
                    {
                        s = ruler;
                    }
                }

                switch (i)
                {
                    case 1:
                        panel1.GetComponent<Image>().sprite = s;
                        break;
                    case 2:
                        panel2.GetComponent<Image>().sprite = s;
                        break;
                    case 3:
                        panel3.GetComponent<Image>().sprite = s;
                        break;
                    case 4:
                        panel4.GetComponent<Image>().sprite = s;
                        break;
                    case 5:
                        panel5.GetComponent<Image>().sprite = s;
                        break;
                    default:
                        Debug.Log("NO SPRITE UPDATE");
                        break;


                }
            }
        }
	}
}