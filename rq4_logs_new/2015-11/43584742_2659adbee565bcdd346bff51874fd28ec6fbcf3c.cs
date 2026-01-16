using UnityEngine;
using System.Collections;

public class ButtonScript : MonoBehaviour {

    public int ID;
    [HideInInspector]
    public int triggered;


    void OnTriggerEnter2D(Collider2D col)
    {
        if (col.gameObject.tag == "filleBody" || col.gameObject.tag == "pereBody")
        {
            triggered += 1;
        }
    }
    void OnTriggerExit2D(Collider2D col)
    {
        if (col.gameObject.tag == "filleBody" || col.gameObject.tag == "pereBody")
        {
            triggered -= 1;
        }
    }
}