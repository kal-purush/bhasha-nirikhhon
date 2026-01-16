using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class HowToPlayScript : MonoBehaviour 
{
    public Text howTo;

    [TextArea(3, 10)]
    public string Maths_How_To = "Help Squelch calculate space travel by placing the correct number into the ships compter sytems!";
    [TextArea(3, 10)]
    public string Physics_How_To = "Help the good critters of Squelch's ship find their way back to the Ships core by creating a path using a range of blocks.";
    [TextArea(3, 10)]
    public string Reflex_How_To = "Bad critters are infecting the local farm lands! Quickly bop them on the head!";
    [TextArea(3, 10)]
    public string Collect_How_To = "The fuel from the ship has landed in a nearby field, maneuver Squelch around the maze and collect as much fuel as possible.";

    public void storyChanger()
    {
        switch ( GameObject.FindObjectOfType<GameController>().WorldPrefix )
        {
            case "Maths":
                howTo.text = Maths_How_To;
                break;
            case "Physics":
                howTo.text = Physics_How_To;
                break;
            case "Collect":
                howTo.text = Collect_How_To;
                break;
            case "Reflex":
                howTo.text = Reflex_How_To;
                break;
        }
    }
}