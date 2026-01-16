using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MainMenuScript : MonoBehaviour {

    public GameObject P1Cursor;
    public GameObject P2Cursor;
    public GameObject P3Cursor;
    public GameObject P4Cursor;

    public bool P1Playing;
    public bool P2Playing;
    public bool P3Playing;
    public bool P4Playing;


    // Use this for initialization
    void Start ()
    {
		
	}
	
	// Update is called once per frame
	void Update ()
    {
        if (Input.GetButtonDown("P1Fire"))
        {
            if (P1Playing == false)
            {
                P1Playing = true;
                Color tmp = P1Cursor.GetComponent<SpriteRenderer>().color;
                tmp.a = 1f;
                P1Cursor.GetComponent<SpriteRenderer>().color = tmp;
            }
            else
            {
                P1Playing = false;
                Color tmp = P1Cursor.GetComponent<SpriteRenderer>().color;
                tmp.a = .25f;
                P1Cursor.GetComponent<SpriteRenderer>().color = tmp;
            }       
        }
        if (Input.GetButtonDown("P2Fire"))
        {
            if (P2Playing == false)
            {
                P2Playing = true;
                Color tmp = P2Cursor.GetComponent<SpriteRenderer>().color;
                tmp.a = 1f;
                P2Cursor.GetComponent<SpriteRenderer>().color = tmp;
            }
            else
            {
                P2Playing = false;
                Color tmp = P2Cursor.GetComponent<SpriteRenderer>().color;
                tmp.a = .25f;
                P2Cursor.GetComponent<SpriteRenderer>().color = tmp;
            }
        }
        if (Input.GetButtonDown("P3Fire"))
        {
            if (P3Playing == false)
            {
                P3Playing = true;
                Color tmp = P3Cursor.GetComponent<SpriteRenderer>().color;
                tmp.a = 1f;
                P3Cursor.GetComponent<SpriteRenderer>().color = tmp;
            }
            else
            {
                P3Playing = false;
                Color tmp = P3Cursor.GetComponent<SpriteRenderer>().color;
                tmp.a = .25f;
                P3Cursor.GetComponent<SpriteRenderer>().color = tmp;
            }
        }
        if (Input.GetButtonDown("P4Fire"))
        {
            if (P4Playing == false)
            {
                P4Playing = true;
                Color tmp = P4Cursor.GetComponent<SpriteRenderer>().color;
                tmp.a = 1f;
                P4Cursor.GetComponent<SpriteRenderer>().color = tmp;
            }
            else
            {
                P4Playing = false;
                Color tmp = P4Cursor.GetComponent<SpriteRenderer>().color;
                tmp.a = .25f;
                P4Cursor.GetComponent<SpriteRenderer>().color = tmp;
            }
        }

        if (Input.GetButtonDown("Submit"))
        {
            if (P1Playing == true || P2Playing == true || P3Playing == true || P4Playing == true)
            {
                Application.LoadLevel(2);
            }
        }


    }






}