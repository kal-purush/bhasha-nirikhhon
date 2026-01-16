using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FlashTutorialSelect : MonoBehaviour
{
    public int flashTimer = 60;

	// Use this for initialization
	void Start ()
    {
		
	}
	
	// Update is called once per frame
	void Update ()
    {
		if(flashTimer > 0)
        {
            flashTimer--;
        }
        else
        {
            if(gameObject.activeSelf)
            {
                gameObject.SetActive(false);
            }
            else
            {
                gameObject.SetActive(true);
            }

            flashTimer++;
        }
	}
}