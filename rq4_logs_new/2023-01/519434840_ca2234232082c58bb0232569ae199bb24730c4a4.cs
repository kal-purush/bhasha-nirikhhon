using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;
using TMPro;

public class HelpBar : MonoBehaviour
{
    private GameObject[] currentHelpButton;

    [SerializeField] private Text upgradeName;
    [SerializeField] private Text upgradeDescription;

    private string[] upgradeNames = new string[]
    { 
        "Strength",
        "Defense",
        "Health", 
        "Speed", 
        "Cooldown", 
        "Starting Dice Number", 
        "Wheat Droprate", 
        "Dice Previewer", 
        "Dice Droprate"
    };

    private string[] upgradeDescriptions = new string[]
    {
        "All of your attacks deal more damage. Each upgrade increases your attack by 50%",
        "You take less damage from most enemy attacks. Each upgrade reduces enemy attack by 25%",
        "Your maximum health increases. The more you upgrade, the more your maximum health increases per upgrade",
        "Your movement speed increases. Each upgrade increases your speed by 10%",
        "The waiting time between firing dice reduces. Each upgrade reduces 10% of the cooldown",
        "Increases the amount of dice you start each run with. The more you upgrade, the more dice you get per upgrade",
        "Increases chance of enemy dropping the wheat currency. Each upgrade increases wheat droprate by 10%",
        "Allows you to view upcoming dice values. Each upgrade lets you view 1 more upcoming dice value, up to 5 dice",
        "Increases chance of enemy dropping a dice. The more you upgrade, the more your dice droprate increases per upgrade"
    };

    void Start()
    {
        currentHelpButton = GameObject.FindGameObjectsWithTag("HelpButton");
    }

    void Update()
    {
        GameObject pressed = EventSystem.current.currentSelectedGameObject;
        if (pressed != currentHelpButton[0] && pressed != currentHelpButton[1] && pressed != currentHelpButton[2] && pressed != currentHelpButton[3] && pressed != currentHelpButton[4] && 
        pressed != currentHelpButton[5] && pressed != currentHelpButton[6] && pressed != currentHelpButton[7] && pressed != currentHelpButton[8] && pressed != this.gameObject)
        {
            this.gameObject.SetActive(false);
        } else
        {
            this.gameObject.SetActive(true);
        }
    }

    public void ShowHelp(int HelpIndex)
    {
        upgradeName.text = upgradeNames[HelpIndex];
        upgradeDescription.text = upgradeDescriptions[HelpIndex];
    }
}