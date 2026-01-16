using System.Collections.Generic;
using UnityEngine;

public class TutorialScript : MonoBehaviour
{

    [System.Serializable]
    public class TutItem
    {
        public Material mat;
    }

    public List<TutItem> tutorialItems = new List<TutItem>();

    private bool inTrigger = false;
    private float tutAlpha = 0;
    private float tutVelocity = 0;

    //fade the material in if player is in trigger and fade it out if the player is out of trigger
    void Update()
    {
        if (inTrigger)
        {
            tutAlpha = Mathf.SmoothDamp(tutAlpha, 1, ref tutVelocity, 1);
            for (int i = 0; i <= tutorialItems.Count - 1; i++) tutorialItems[i].mat.SetColor("_Color", new Color(0,0,0,tutAlpha));
        }
        else
        {
            tutAlpha = Mathf.SmoothDamp(tutAlpha, 0, ref tutVelocity, 0.6f);
            for (int i = 0; i <= tutorialItems.Count - 1; i++) tutorialItems[i].mat.SetColor("_Color", new Color(0, 0, 0, tutAlpha));
        }
    }

    private void OnTriggerStay(Collider collider)
    {
        if (collider.gameObject.tag == "Player") inTrigger = true;
    }

    private void OnTriggerExit(Collider collider)
    {
        if (collider.gameObject.tag == "Player") inTrigger = false;
    }
}