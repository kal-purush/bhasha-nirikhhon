using UnityEngine;
using System.Collections;

public class BlueScript : MonoBehaviour
{
    public Main main;
    public Main0 main0;
    // Use this for initialization
   public void pickACube(string s)
    {
        if (s == "Red")
        {
            Debug.Log("clicked 0");

            main.testCorrect(0);
        }
        else if (s == "Blue")
        {
            Debug.Log("clicked 2");
            main.testCorrect(2);
        }
        else if (s == "Yellow")
        {
            //Debug.Log("clicked 3");
            main.testCorrect(3);
        }
        else if (s == "Green")
        {
            // Debug.Log("clicked 1");
            main.testCorrect(1);
        }
        else if (s == "Red0")
        {
            Debug.Log("pressed on red0");
            main0.testCorrect(0);
            Debug.Log("after red0 being pressed");
        }
        else if (s == "Blue0")
        {
            main0.testCorrect(2);
        }
    }
}