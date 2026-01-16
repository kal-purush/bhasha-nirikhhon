using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using TMPro;

public class Objects : MonoBehaviour
{
    private bool firstInteraction = false;
    private string thisObject;


    [SerializeField] private TextMeshProUGUI objectText;
    [SerializeField] private GameObject Button;

    // Start is called before the first frame update
    void Start()
    {
        thisObject = this.name;
    }

    public void DisplayStory()
    {
        DisplayInteractionText(firstInteraction);

        if (firstInteraction)
        {
            //DisplayInteractionText(firstInteraction);
            firstInteraction = false;
        }
    }


    private void DisplayInteractionText(bool currentInteraction)
    {
        string textToDisplay;
        
        if (currentInteraction)
        {
            textToDisplay = DisplayFirstText();
        }
        else
        {
           textToDisplay = DisplaySecondText();
        }

        objectText.text = textToDisplay;
    }

    private string DisplayFirstText()
    {
        switch (thisObject)
        {
            case "Bible":
                return "James - reads every night";
            case "Radio":
                return "B - listening to radio, some man giving a speech - muffled";
            case "JazzRecord":
                return "Kalu - Checking for scratches and playing";
            case "Handgun":
                return "Thandi - loaded with bullets";
            case "BoxingGlove":
                return "Roli - Belongs tp him, lost the other while crossing river into botswana";
            case "ContrabanPaper":
                return "Roli - writing letter to brother who is a prison warden";
            case "PoliceBaton":
                return "Thandi - stole from state police";
            case "DrumMagazine":
                return "Kalu - Crush on Miriam Makeba, reminds him of home";
            default:
                return "No information to display on Object.";
        }
        
    }

    private string DisplaySecondText()
    {
        switch (thisObject)
        {
            case "Bible":
                return "Thandi - Tore a page to roll tobacco";
            case "Radio":
                return "James - changing channels and doing some tuning";
            case "JazzRecord":
                return "B - switching off record player and returning it to cover";
            case "Handgun":
                return "Roli - thinking abour how bro who is a prison warden would never hold a gun";
            case "BoxingGlove":
                return "Kalu - doesn't like boxing, not physically fit.";
            case "ContrabanPaper":
                return "James - writing letter to unkown adress";
            case "PoliceBaton":
                return "Roli -repulsed, reminds him of worst qualities of his brother";
            case "DrumMagazine":
                return "James - hates that Miriam Makeba had a nine day marriage, she is not a god-fearing woman";
            default:
                return "No information to display on Object.";
        }

    }


    /*// Update is called once per frame
    void Update()
    {
        
    }*/

    private void OnMouseDown()
    {
        Button.SetActive(true);
    }
}