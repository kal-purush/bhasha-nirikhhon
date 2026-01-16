using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class directToSite : MonoBehaviour
{
    public void playGame()  //load the game scene
    {
        SceneManager.LoadScene(1);
    }
    public void openFacebook()  //goes to my facebook   
    {
        Application.OpenURL("https://www.facebook.com/Nightey3s");
    }
    public void openTwitter()   //goes to my twitter
    {
        Application.OpenURL("https://twitter.com/Nightey3s");
    }
    public void openFAQ()   //open faq page
    {
        SceneManager.LoadScene(2);
    }
}