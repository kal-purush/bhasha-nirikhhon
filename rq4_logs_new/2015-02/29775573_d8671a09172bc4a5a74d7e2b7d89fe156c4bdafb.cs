using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class Cen03_door : MonoBehaviour {
    
    public string text;

    public Text actionText;
    public Text actionText_shadow;

    void OnTriggerStay(Collider other)
    {
        if (other.name == "Char")
        {
            actionText_shadow.text = actionText.text = text;
            if (Input.GetKey(KeyCode.Space))
            {
                Application.LoadLevel("Cen05_Parque");
            }
        }
    }

    void OnTriggerExit(Collider other)
    {
        if (other.name == "Char")
        {
            actionText_shadow.text = actionText.text = "";
        }
    }

}