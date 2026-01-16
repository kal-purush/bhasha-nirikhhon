using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System.Collections;


public class TutorialControl : MonoBehaviour
{
    public GameObject tutorialWood1;
    public GameObject tutorialWood2;
    public GameObject tutorialShirt1;
    public GameObject tutorialShirt2;

    private void Start()
    {
        FindObjectOfType<CanvasControl>().SetOverlayText("Find all of the tall pine tress, chop them down to gather wood and build your cabin.", 7);
    }

    public void ShowShirt1()
    {
        tutorialShirt1.SetActive(true);
    }
    public void ShowShirt2()
    {
        tutorialShirt2.SetActive(true);
    }
    public void ShowTree2()
    {
        tutorialWood2.SetActive(true);
    }

}