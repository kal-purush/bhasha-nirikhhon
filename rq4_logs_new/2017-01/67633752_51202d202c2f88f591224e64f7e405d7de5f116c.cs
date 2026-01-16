using UnityEngine;
using System.Collections;

public class Tavern : MonoBehaviour {
    TownWatchdog townWatchdog;
    public GameObject restPopUp;
    public GameObject errorPopUp;
	// Use this for initialization
	void Start () {
        townWatchdog = GetComponent<TownWatchdog>();
	}

    public void rest() {
        if (Player.gold.Value >= 50)
        {
            Player.giveHealth(Player.maxHealth);
            Player.takeGold(50);
        }
        else
        {
            errorPopUp.SetActive(true);
            print("Not enough gold.");
        }
            
    }

    public void openTavernPopUp()
    {
        if (townWatchdog.menus.activeSelf == true)
            return;
        townWatchdog.menus.SetActive(true);
        townWatchdog.tavern.SetActive(true);
        townWatchdog.exitButton.SetActive(true);
    }
	
    public void closeRestPopUp()
    {
        restPopUp.SetActive(false);
    }

    public void openRestPopUp()
    {
        restPopUp.SetActive(true);
    }
	// Update is called once per frame
	void Update () {
	
	}
}